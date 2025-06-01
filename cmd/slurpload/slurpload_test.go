package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/chtzvt/certslurp/internal/extractor"
	"github.com/dsnet/compress/bzip2"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

/*
To run the integration tests, you’ll need a running PostgreSQL instance.
The easiest way is to use Docker to spin up a disposable container:

    docker run --rm \
      -e POSTGRES_USER=slurper \
      -e POSTGRES_PASSWORD=slurper \
      -e POSTGRES_DB=slurper_test \
      -p 5433:5432 \
	  --shm-size=1g \
      --name slurper-test-postgres \
      postgres:latest

This starts a PostgreSQL server on localhost:5433 with the username `slurper`, password `slurper`, and database `slurper_test`.
You can stop it any time with:

    docker stop slurper-test-postgres

In a separate shell, set the following environment variable so your Go tests connect to this instance:

    export TEST_DATABASE_DSN="host=localhost port=5433 user=slurper password=slurper dbname=slurper_test sslmode=disable"

Now just run your tests as usual:

    go test ./...

Note: Adjust the port if 5433 is taken, but make sure it matches in your DSN.
*/

func setupTestDB(t *testing.T) *sql.DB {
	dsn := os.Getenv("TEST_DATABASE_DSN")
	if dsn == "" {
		t.Fatal("TEST_DATABASE_DSN not set")
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)

	_, err = db.Exec("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
	require.NoError(t, err)

	require.NoError(t, runInitDB(db))

	fqdnCache, err = initFQDNLRUCache(1000)
	require.NoError(t, err)

	return db
}

func teardownTestDB(t *testing.T, db *sql.DB) {
	require.NoError(t, db.Close())
	fqdnCache = nil
}

func compressGzip(data []byte) []byte {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, _ = w.Write(data)
	w.Close()
	return buf.Bytes()
}

func compressBzip2(data []byte) []byte {
	var buf bytes.Buffer
	w, _ := bzip2.NewWriter(&buf, &bzip2.WriterConfig{Level: bzip2.BestCompression})
	_, _ = w.Write(data)
	w.Close()
	return buf.Bytes()
}

func writeTestFile(t *testing.T, dir, ext, data string) string {
	path := filepath.Join(dir, "test"+ext)
	switch ext {
	case ".jsonl":
		require.NoError(t, os.WriteFile(path, []byte(data), 0644))
	case ".jsonl.gz":
		f, err := os.Create(path)
		require.NoError(t, err)
		gz := gzip.NewWriter(f)
		_, err = gz.Write([]byte(data))
		require.NoError(t, err)
		require.NoError(t, gz.Close())
		require.NoError(t, f.Close())
	case ".jsonl.bz2":
		f, err := os.Create(path)
		require.NoError(t, err)
		bz, err := bzip2.NewWriter(f, nil)
		require.NoError(t, err)
		_, err = bz.Write([]byte(data))
		require.NoError(t, err)
		require.NoError(t, bz.Close())
		require.NoError(t, f.Close())
	}
	return path
}

const testJsonl = `
{"cn":"gzmfjc.cn","dns":["gzmfjc.cn","www.gzmfjc.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504410,"naf":"2022-03-22T23:59:59Z","nbf":"2021-03-22T00:00:00Z","sub":"CN=gzmfjc.cn","t":"cert"}
{"cn":"ots.ikuaixue.cn","dns":["ots.ikuaixue.cn"],"iss":"TrustAsia TLS RSA CA","li":26504411,"naf":"2022-07-14T23:59:59Z","nbf":"2021-07-07T00:00:00Z","sub":"CN=ots.ikuaixue.cn","t":"cert"}
{"cn":"h-20.cn","dns":["h-20.cn","*.h-20.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504386,"naf":"2022-06-30T23:59:59Z","nbf":"2021-06-30T00:00:00Z","sub":"CN=h-20.cn","t":"cert"}
{"cn":"www.cgbjfts.com","dns":["www.cgbjfts.com","cgbjfts.com"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504412,"naf":"2022-07-17T23:59:59Z","nbf":"2021-07-17T00:00:00Z","sub":"CN=www.cgbjfts.com","t":"cert"}
{"cn":"ac.haohao99.cn","dns":["ac.haohao99.cn"],"iss":"TrustAsia TLS RSA CA","li":26504414,"naf":"2022-06-21T23:59:59Z","nbf":"2021-06-22T00:00:00Z","sub":"CN=ac.haohao99.cn","t":"cert"}
{"cn":"www.hackerstart.cn","dns":["www.hackerstart.cn","hackerstart.cn"],"iss":"TrustAsia TLS RSA CA","li":26504415,"naf":"2022-07-03T23:59:59Z","nbf":"2021-07-04T00:00:00Z","sub":"CN=www.hackerstart.cn","t":"cert"}
{"cn":"www.gzhx-tec.cn","dns":["www.gzhx-tec.cn","gzhx-tec.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504403,"naf":"2022-04-13T23:59:59Z","nbf":"2021-04-13T00:00:00Z","sub":"CN=www.gzhx-tec.cn","t":"cert"}
{"cn":"www.hansent.cn","dns":["www.hansent.cn","hansent.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504413,"naf":"2022-07-16T23:59:59Z","nbf":"2021-07-16T00:00:00Z","sub":"CN=www.hansent.cn","t":"cert"}
{"cn":"en.hansedu.cn","dns":["en.hansedu.cn"],"iss":"TrustAsia TLS RSA CA","li":26504406,"naf":"2022-07-17T23:59:59Z","nbf":"2021-07-18T00:00:00Z","sub":"CN=en.hansedu.cn","t":"cert"}
{"cn":"ga.ffggte.cn","dns":["ga.ffggte.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504163,"naf":"2022-07-19T23:59:59Z","nbf":"2021-07-19T00:00:00Z","sub":"CN=ga.ffggte.cn","t":"cert"}
{"cn":"fiqz.cn","dns":["fiqz.cn","www.fiqz.cn"],"iss":"TrustAsia TLS RSA CA","li":26504161,"naf":"2022-07-20T23:59:59Z","nbf":"2021-07-21T00:00:00Z","sub":"CN=fiqz.cn","t":"cert"}
{"cn":"flimg.cn","dns":["flimg.cn","www.flimg.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504164,"naf":"2022-07-19T23:59:59Z","nbf":"2021-07-19T00:00:00Z","sub":"CN=flimg.cn","t":"cert"}
{"cn":"app.fnscore.cn","dns":["app.fnscore.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504160,"naf":"2022-06-23T23:59:59Z","nbf":"2021-06-23T00:00:00Z","sub":"CN=app.fnscore.cn","t":"cert"}
{"cn":"*.flyread.cn","dns":["*.flyread.cn","flyread.cn"],"iss":"RapidSSL TLS DV RSA Mixed SHA256 2020 CA-1","li":26504166,"naf":"2022-07-20T23:59:59Z","nbf":"2021-07-20T00:00:00Z","sub":"CN=*.flyread.cn","t":"cert"}
{"cn":"www.freerangers.cn","dns":["www.freerangers.cn","freerangers.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504167,"naf":"2022-05-27T23:59:59Z","nbf":"2021-05-27T00:00:00Z","sub":"CN=www.freerangers.cn","t":"cert"}
{"cn":"www.forkai.cn","dns":["www.forkai.cn","forkai.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504162,"naf":"2022-07-17T23:59:59Z","nbf":"2021-07-17T00:00:00Z","sub":"CN=www.forkai.cn","t":"cert"}
{"cn":"www.famule.cn","dns":["www.famule.cn","famule.cn"],"iss":"TrustAsia TLS RSA CA","li":26504169,"naf":"2022-05-12T23:59:59Z","nbf":"2021-05-13T00:00:00Z","sub":"CN=www.famule.cn","t":"cert"}
{"cn":"fvcx.cn","dns":["fvcx.cn","www.fvcx.cn"],"iss":"TrustAsia TLS RSA CA","li":26504170,"naf":"2022-07-21T23:59:59Z","nbf":"2021-07-22T00:00:00Z","sub":"CN=fvcx.cn","t":"cert"}
{"cn":"git.gzquan.cn","dns":["git.gzquan.cn"],"iss":"TrustAsia TLS RSA CA","li":26504168,"naf":"2022-07-14T23:59:59Z","nbf":"2021-07-15T00:00:00Z","sub":"CN=git.gzquan.cn","t":"cert"}
{"cn":"dd1.freep.cn","dns":["dd1.freep.cn"],"iss":"TrustAsia TLS RSA CA","li":26504172,"naf":"2022-07-21T23:59:59Z","nbf":"2021-07-22T00:00:00Z","sub":"CN=dd1.freep.cn","t":"cert"}
{"cn":"furryhome.cn","dns":["furryhome.cn","www.furryhome.cn"],"iss":"TrustAsia TLS RSA CA","li":26504173,"naf":"2022-02-13T23:59:59Z","nbf":"2021-02-14T00:00:00Z","sub":"CN=furryhome.cn","t":"cert"}
{"cn":"furcon.fursuit.cn","dns":["furcon.fursuit.cn"],"iss":"TrustAsia TLS RSA CA","li":26504175,"naf":"2022-04-12T23:59:59Z","nbf":"2021-04-13T00:00:00Z","sub":"CN=furcon.fursuit.cn","t":"cert"}
{"cn":"www.foodpanel.cn","dns":["www.foodpanel.cn","foodpanel.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504176,"naf":"2022-07-15T23:59:59Z","nbf":"2021-07-15T00:00:00Z","sub":"CN=www.foodpanel.cn","t":"cert"}
{"cn":"fitl.cn","dns":["fitl.cn","www.fitl.cn"],"iss":"TrustAsia TLS RSA CA","li":26504177,"naf":"2022-07-20T23:59:59Z","nbf":"2021-07-21T00:00:00Z","sub":"CN=fitl.cn","t":"cert"}
{"cn":"www.fvfj.cn","dns":["www.fvfj.cn","fvfj.cn"],"iss":"TrustAsia TLS RSA CA","li":26504178,"naf":"2022-07-21T23:59:59Z","nbf":"2021-07-22T00:00:00Z","sub":"CN=www.fvfj.cn","t":"cert"}
{"cn":"fyed.cn","dns":["fyed.cn","www.fyed.cn"],"iss":"TrustAsia TLS RSA CA","li":26504179,"naf":"2022-07-20T23:59:59Z","nbf":"2021-07-21T00:00:00Z","sub":"CN=fyed.cn","t":"cert"}
{"cn":"www.fxcdn.cn","dns":["www.fxcdn.cn","fxcdn.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504180,"naf":"2022-03-30T23:59:59Z","nbf":"2021-03-30T00:00:00Z","sub":"CN=www.fxcdn.cn","t":"cert"}
{"cn":"fyel.cn","dns":["fyel.cn","www.fyel.cn"],"iss":"TrustAsia TLS RSA CA","li":26504181,"naf":"2022-07-20T23:59:59Z","nbf":"2021-07-21T00:00:00Z","sub":"CN=fyel.cn","t":"cert"}
{"cn":"git.fulongtech.cn","dns":["git.fulongtech.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504182,"naf":"2021-12-15T23:59:59Z","nbf":"2020-12-15T00:00:00Z","sub":"CN=git.fulongtech.cn","t":"cert"}
{"cn":"fyci.cn","dns":["fyci.cn","www.fyci.cn"],"iss":"TrustAsia TLS RSA CA","li":26504183,"naf":"2022-07-20T23:59:59Z","nbf":"2021-07-21T00:00:00Z","sub":"CN=fyci.cn","t":"cert"}
{"cn":"fxxo.cn","dns":["fxxo.cn","www.fxxo.cn"],"iss":"TrustAsia TLS RSA CA","li":26504184,"naf":"2022-07-20T23:59:59Z","nbf":"2021-07-21T00:00:00Z","sub":"CN=fxxo.cn","t":"cert"}
{"cn":"www.fybe.cn","dns":["www.fybe.cn","fybe.cn"],"iss":"TrustAsia TLS RSA CA","li":26504185,"naf":"2022-07-20T23:59:59Z","nbf":"2021-07-21T00:00:00Z","sub":"CN=www.fybe.cn","t":"cert"}
{"cn":"fvfo.cn","dns":["fvfo.cn","www.fvfo.cn"],"iss":"TrustAsia TLS RSA CA","li":26504186,"naf":"2022-07-21T23:59:59Z","nbf":"2021-07-22T00:00:00Z","sub":"CN=fvfo.cn","t":"cert"}
{"cn":"fygi.cn","dns":["fygi.cn","www.fygi.cn"],"iss":"TrustAsia TLS RSA CA","li":26504187,"naf":"2022-07-20T23:59:59Z","nbf":"2021-07-21T00:00:00Z","sub":"CN=fygi.cn","t":"cert"}
{"cn":"vip.fxinz.cn","dns":["vip.fxinz.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504188,"naf":"2022-07-17T23:59:59Z","nbf":"2021-07-17T00:00:00Z","sub":"CN=vip.fxinz.cn","t":"cert"}
{"cn":"dzaa00.cn","dns":["dzaa00.cn","www.dzaa00.cn"],"iss":"TrustAsia TLS RSA CA","li":26504189,"naf":"2022-07-19T23:59:59Z","nbf":"2021-07-20T00:00:00Z","sub":"CN=dzaa00.cn","t":"cert"}
{"cn":"fydo.cn","dns":["fydo.cn","www.fydo.cn"],"iss":"TrustAsia TLS RSA CA","li":26504190,"naf":"2022-07-20T23:59:59Z","nbf":"2021-07-21T00:00:00Z","sub":"CN=fydo.cn","t":"cert"}
{"cn":"feae.cn","dns":["feae.cn","www.feae.cn"],"iss":"TrustAsia TLS RSA CA","li":26504165,"naf":"2022-07-20T23:59:59Z","nbf":"2021-07-21T00:00:00Z","sub":"CN=feae.cn","t":"cert"}
{"cn":"www.fsgo365.cn","dns":["www.fsgo365.cn","fsgo365.cn"],"iss":"TrustAsia TLS RSA CA","li":26504174,"naf":"2022-05-19T23:59:59Z","nbf":"2021-05-20T00:00:00Z","sub":"CN=www.fsgo365.cn","t":"cert"}
{"cn":"m.fsyk.cn","dns":["m.fsyk.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504171,"naf":"2022-07-20T23:59:59Z","nbf":"2021-07-20T00:00:00Z","sub":"CN=m.fsyk.cn","t":"cert"}
{"cn":"res.gamehz.cn","dns":["res.gamehz.cn"],"iss":"TrustAsia TLS RSA CA","li":26504191,"naf":"2022-08-04T23:59:59Z","nbf":"2021-07-20T00:00:00Z","sub":"CN=res.gamehz.cn","t":"cert"}
{"cn":"dshhyyey.ksecloud.cn","dns":["dshhyyey.ksecloud.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504867,"naf":"2022-07-19T23:59:59Z","nbf":"2021-07-19T00:00:00Z","sub":"CN=dshhyyey.ksecloud.cn","t":"cert"}
{"cn":"kouleen.cn","dns":["kouleen.cn","www.kouleen.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504866,"naf":"2022-01-08T23:59:59Z","nbf":"2021-01-08T00:00:00Z","sub":"CN=kouleen.cn","t":"cert"}
{"cn":"www.kcdemo.cn","dns":["www.kcdemo.cn","kcdemo.cn"],"iss":"TrustAsia TLS RSA CA","li":26504865,"naf":"2022-04-26T23:59:59Z","nbf":"2021-04-27T00:00:00Z","sub":"CN=www.kcdemo.cn","t":"cert"}
{"cn":"lxxx.ksecloud.cn","dns":["lxxx.ksecloud.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504868,"naf":"2022-07-19T23:59:59Z","nbf":"2021-07-19T00:00:00Z","sub":"CN=lxxx.ksecloud.cn","t":"cert"}
{"cn":"kongqibaba.cn","dns":["kongqibaba.cn","www.kongqibaba.cn"],"iss":"TrustAsia TLS RSA CA","li":26504871,"naf":"2022-07-12T23:59:59Z","nbf":"2021-07-13T00:00:00Z","sub":"CN=kongqibaba.cn","t":"cert"}
{"cn":"sc.lemonbrothers.cn","dns":["sc.lemonbrothers.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504864,"naf":"2022-05-14T23:59:59Z","nbf":"2021-05-14T00:00:00Z","sub":"CN=sc.lemonbrothers.cn","t":"cert"}
{"cn":"api.tanmantang.com","dns":["api.tanmantang.com"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504872,"naf":"2022-07-04T23:59:59Z","nbf":"2021-07-04T00:00:00Z","sub":"CN=api.tanmantang.com","t":"cert"}
{"cn":"ks93.cn","dns":["ks93.cn","www.ks93.cn"],"iss":"TrustAsia TLS RSA CA","li":26504869,"naf":"2022-07-14T23:59:59Z","nbf":"2021-07-15T00:00:00Z","sub":"CN=ks93.cn","t":"cert"}
{"cn":"www.keqiao520.cn","dns":["www.keqiao520.cn","keqiao520.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504874,"naf":"2022-07-17T23:59:59Z","nbf":"2021-07-17T00:00:00Z","sub":"CN=www.keqiao520.cn","t":"cert"}
{"cn":"kycloud3.koyoo.cn","dns":["kycloud3.koyoo.cn"],"iss":"TrustAsia TLS RSA CA","li":26504870,"naf":"2022-07-06T23:59:59Z","nbf":"2021-07-07T00:00:00Z","sub":"CN=kycloud3.koyoo.cn","t":"cert"}
{"cn":"lcl101.cn","dns":["lcl101.cn","www.lcl101.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504875,"naf":"2022-07-16T23:59:59Z","nbf":"2021-07-16T00:00:00Z","sub":"CN=lcl101.cn","t":"cert"}
{"cn":"*.fitnessdiving.com","dns":["*.fitnessdiving.com"],"iss":"Amazon","li":26504873,"naf":"2022-07-16T23:59:59Z","nbf":"2021-06-17T00:00:00Z","sub":"CN=*.fitnessdiving.com","t":"cert"}
{"cn":"public.kpjushi.cn","dns":["public.kpjushi.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504878,"naf":"2022-07-15T23:59:59Z","nbf":"2021-07-15T00:00:00Z","sub":"CN=public.kpjushi.cn","t":"cert"}
{"cn":"ksis.ksecloud.cn","dns":["ksis.ksecloud.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504879,"naf":"2022-07-16T23:59:59Z","nbf":"2021-07-16T00:00:00Z","sub":"CN=ksis.ksecloud.cn","t":"cert"}
{"cn":"tongchengcn.kukexinmei.cn","dns":["tongchengcn.kukexinmei.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504876,"naf":"2022-04-13T23:59:59Z","nbf":"2021-04-13T00:00:00Z","sub":"CN=tongchengcn.kukexinmei.cn","t":"cert"}
{"cn":"m.lanpeinfo.cn","dns":["m.lanpeinfo.cn"],"iss":"TrustAsia TLS RSA CA","li":26504881,"naf":"2022-07-20T23:59:59Z","nbf":"2021-07-14T00:00:00Z","sub":"CN=m.lanpeinfo.cn","t":"cert"}
{"cn":"www.killzero.cn","dns":["www.killzero.cn","killzero.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504882,"naf":"2022-07-17T23:59:59Z","nbf":"2021-07-17T00:00:00Z","sub":"CN=www.killzero.cn","t":"cert"}
{"cn":"t.kuailon.cn","dns":["t.kuailon.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504880,"naf":"2022-07-20T23:59:59Z","nbf":"2021-07-20T00:00:00Z","sub":"CN=t.kuailon.cn","t":"cert"}
{"cn":"www.kuwow.cn","dns":["www.kuwow.cn","kuwow.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504884,"naf":"2022-07-02T23:59:59Z","nbf":"2021-07-02T00:00:00Z","sub":"CN=www.kuwow.cn","t":"cert"}
{"cn":"c.kuaitouhui.cn","dns":["c.kuaitouhui.cn"],"iss":"TrustAsia TLS RSA CA","li":26504877,"naf":"2021-08-31T12:00:00Z","nbf":"2020-08-31T00:00:00Z","sub":"CN=c.kuaitouhui.cn","t":"cert"}
{"cn":"killzero.cn","dns":["killzero.cn","www.killzero.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504883,"naf":"2022-07-16T23:59:59Z","nbf":"2021-07-16T00:00:00Z","sub":"CN=killzero.cn","t":"cert"}
{"cn":"www.kuangyizu.cn","dns":["www.kuangyizu.cn","kuangyizu.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504887,"naf":"2021-12-29T23:59:59Z","nbf":"2020-12-29T00:00:00Z","sub":"CN=www.kuangyizu.cn","t":"cert"}
{"cn":"ksjxzx.ksecloud.cn","dns":["ksjxzx.ksecloud.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504885,"naf":"2022-07-19T23:59:59Z","nbf":"2021-07-19T00:00:00Z","sub":"CN=ksjxzx.ksecloud.cn","t":"cert"}
{"cn":"scjy.laoyougj.cn","dns":["scjy.laoyougj.cn"],"iss":"TrustAsia TLS RSA CA","li":26504889,"naf":"2021-11-12T23:59:59Z","nbf":"2020-11-13T00:00:00Z","sub":"CN=scjy.laoyougj.cn","t":"cert"}
{"cn":"lrz.liangrizhang.cn","dns":["lrz.liangrizhang.cn"],"iss":"TrustAsia TLS RSA CA","li":26504890,"naf":"2022-01-29T23:59:59Z","nbf":"2021-01-30T00:00:00Z","sub":"CN=lrz.liangrizhang.cn","t":"cert"}
{"cn":"rockbyte.cn","dns":["rockbyte.cn","www.rockbyte.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504886,"naf":"2022-07-16T23:59:59Z","nbf":"2021-07-16T00:00:00Z","sub":"CN=rockbyte.cn","t":"cert"}
{"cn":"*.kw13.cn","co":["CN"],"dns":["*.kw13.cn","kw13.cn"],"iss":"GeoTrust CN RSA CA G1","li":26504892,"loc":["广州市"],"naf":"2022-08-10T23:59:59Z","nbf":"2021-07-19T00:00:00Z","org":["广东快问信息科技有限公司"],"prv":["广东省"],"sub":"CN=*.kw13.cn,O=广东快问信息科技有限公司,L=广州市,ST=广东省,C=CN","t":"cert"}
{"cn":"kongqy.cn","dns":["kongqy.cn","www.kongqy.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504891,"naf":"2022-07-09T23:59:59Z","nbf":"2021-07-09T00:00:00Z","sub":"CN=kongqy.cn","t":"cert"}
{"cn":"cos.kxfan.cn","dns":["cos.kxfan.cn"],"iss":"TrustAsia TLS RSA CA","li":26504893,"naf":"2022-07-20T23:59:59Z","nbf":"2021-07-21T00:00:00Z","sub":"CN=cos.kxfan.cn","t":"cert"}
{"cn":"lanqingqing.cn","dns":["lanqingqing.cn","www.lanqingqing.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504894,"naf":"2022-06-28T23:59:59Z","nbf":"2021-06-28T00:00:00Z","sub":"CN=lanqingqing.cn","t":"cert"}
{"cn":"mg.ledaren.cn","dns":["mg.ledaren.cn"],"iss":"TrustAsia TLS RSA CA","li":26504895,"naf":"2022-07-20T23:59:59Z","nbf":"2021-07-21T00:00:00Z","sub":"CN=mg.ledaren.cn","t":"cert"}
{"cn":"m.ksfs.cn","dns":["m.ksfs.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504888,"naf":"2022-04-28T23:59:59Z","nbf":"2021-04-28T00:00:00Z","sub":"CN=m.ksfs.cn","t":"cert"}
{"cn":"tdata.iimedia.cn","dns":["tdata.iimedia.cn"],"iss":"TrustAsia TLS RSA CA","li":26504643,"naf":"2022-07-16T23:59:59Z","nbf":"2021-07-16T00:00:00Z","sub":"CN=tdata.iimedia.cn","t":"cert"}
{"cn":"sg.ilicloud.cn","dns":["sg.ilicloud.cn"],"iss":"TrustAsia TLS RSA CA","li":26504644,"naf":"2022-06-23T23:59:59Z","nbf":"2021-06-24T00:00:00Z","sub":"CN=sg.ilicloud.cn","t":"cert"}
{"cn":"ikeawell.cn","dns":["ikeawell.cn","www.ikeawell.cn"],"iss":"TrustAsia TLS RSA CA","li":26504645,"naf":"2022-07-07T23:59:59Z","nbf":"2021-07-08T00:00:00Z","sub":"CN=ikeawell.cn","t":"cert"}
{"cn":"www.hsykjz.cn","dns":["www.hsykjz.cn","hsykjz.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504646,"naf":"2022-07-19T23:59:59Z","nbf":"2021-07-19T00:00:00Z","sub":"CN=www.hsykjz.cn","t":"cert"}
{"cn":"*.qa.huohua.cn","dns":["*.qa.huohua.cn"],"iss":"RapidSSL TLS DV RSA Mixed SHA256 2020 CA-1","li":26504647,"naf":"2022-07-26T23:59:59Z","nbf":"2021-06-28T00:00:00Z","sub":"CN=*.qa.huohua.cn","t":"cert"}
{"cn":"*.ijiesheng.cn","dns":["*.ijiesheng.cn","ijiesheng.cn"],"iss":"RapidSSL TLS DV RSA Mixed SHA256 2020 CA-1","li":26504648,"naf":"2022-07-19T23:59:59Z","nbf":"2021-07-19T00:00:00Z","sub":"CN=*.ijiesheng.cn","t":"cert"}
{"cn":"www.ihuilian.cn","dns":["www.ihuilian.cn","ihuilian.cn"],"iss":"TrustAsia TLS RSA CA","li":26504649,"naf":"2022-07-18T23:59:59Z","nbf":"2021-07-19T00:00:00Z","sub":"CN=www.ihuilian.cn","t":"cert"}
{"cn":"bfxy.inter-action.cn","dns":["bfxy.inter-action.cn"],"iss":"TrustAsia TLS RSA CA","li":26504650,"naf":"2022-01-04T23:59:59Z","nbf":"2021-01-05T00:00:00Z","sub":"CN=bfxy.inter-action.cn","t":"cert"}
{"cn":"meet-cloud.metelyd.com","dns":["meet-cloud.metelyd.com"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504651,"naf":"2022-07-21T23:59:59Z","nbf":"2021-07-21T00:00:00Z","sub":"CN=meet-cloud.metelyd.com","t":"cert"}
{"cn":"www.inmore.cn","dns":["www.inmore.cn","inmore.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504652,"naf":"2022-06-07T23:59:59Z","nbf":"2021-06-07T00:00:00Z","sub":"CN=www.inmore.cn","t":"cert"}
{"cn":"www.inspire2030.cn","dns":["www.inspire2030.cn","inspire2030.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504653,"naf":"2022-07-20T23:59:59Z","nbf":"2021-07-20T00:00:00Z","sub":"CN=www.inspire2030.cn","t":"cert"}
{"cn":"innomem.cn","dns":["innomem.cn","www.innomem.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504654,"naf":"2022-06-21T23:59:59Z","nbf":"2021-06-21T00:00:00Z","sub":"CN=innomem.cn","t":"cert"}
{"cn":"k.iruanhui.cn","dns":["k.iruanhui.cn"],"iss":"TrustAsia TLS RSA CA","li":26504655,"naf":"2022-06-26T23:59:59Z","nbf":"2021-06-27T00:00:00Z","sub":"CN=k.iruanhui.cn","t":"cert"}
{"cn":"ipw.cn","dns":["ipw.cn","www.ipw.cn"],"iss":"TrustAsia TLS RSA CA","li":26504656,"naf":"2022-06-30T23:59:59Z","nbf":"2021-07-01T00:00:00Z","sub":"CN=ipw.cn","t":"cert"}
{"cn":"bk.luck.zone","dns":["bk.luck.zone"],"iss":"TrustAsia TLS RSA CA","li":26504657,"naf":"2022-06-23T23:59:59Z","nbf":"2021-06-24T00:00:00Z","sub":"CN=bk.luck.zone","t":"cert"}
{"cn":"imagine-world-wx.imaginelearning.cn","dns":["imagine-world-wx.imaginelearning.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504658,"naf":"2022-05-20T23:59:59Z","nbf":"2021-05-20T00:00:00Z","sub":"CN=imagine-world-wx.imaginelearning.cn","t":"cert"}
{"cn":"cdn.ijimo.cn","dns":["cdn.ijimo.cn"],"iss":"TrustAsia TLS RSA CA","li":26504659,"naf":"2022-06-19T23:59:59Z","nbf":"2021-06-20T00:00:00Z","sub":"CN=cdn.ijimo.cn","t":"cert"}
{"cn":"iocare.cn","dns":["iocare.cn","www.iocare.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504660,"naf":"2022-07-01T23:59:59Z","nbf":"2021-07-01T00:00:00Z","sub":"CN=iocare.cn","t":"cert"}
{"cn":"www.imageaudio.cn","dns":["www.imageaudio.cn","imageaudio.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504642,"naf":"2022-07-02T23:59:59Z","nbf":"2021-07-02T00:00:00Z","sub":"CN=www.imageaudio.cn","t":"cert"}
{"cn":"www.iotbi.cn","dns":["www.iotbi.cn","iotbi.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504640,"naf":"2022-07-21T23:59:59Z","nbf":"2021-07-21T00:00:00Z","sub":"CN=www.iotbi.cn","t":"cert"}
{"cn":"superapp.imzhuan.cn","dns":["superapp.imzhuan.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504641,"naf":"2022-01-28T23:59:59Z","nbf":"2021-01-28T00:00:00Z","sub":"CN=superapp.imzhuan.cn","t":"cert"}
{"cn":"*.jingyang.cn","dns":["*.jingyang.cn"],"iss":"Amazon","li":26504664,"naf":"2022-08-07T23:59:59Z","nbf":"2021-07-09T00:00:00Z","sub":"CN=*.jingyang.cn","t":"cert"}
{"cn":"*.joy999.cn","dns":["*.joy999.cn","joy999.cn"],"iss":"RapidSSL TLS DV RSA Mixed SHA256 2020 CA-1","li":26504665,"naf":"2022-07-16T23:59:59Z","nbf":"2021-07-16T00:00:00Z","sub":"CN=*.joy999.cn","t":"cert"}
{"cn":"www.ispay.cn","dns":["www.ispay.cn","ispay.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504666,"naf":"2022-07-20T23:59:59Z","nbf":"2021-07-20T00:00:00Z","sub":"CN=www.ispay.cn","t":"cert"}
{"cn":"bio.immvira.cn","dns":["bio.immvira.cn"],"iss":"TrustAsia TLS RSA CA","li":26504667,"naf":"2022-06-29T23:59:59Z","nbf":"2021-06-30T00:00:00Z","sub":"CN=bio.immvira.cn","t":"cert"}
{"cn":"www.itciraos.cn","dns":["www.itciraos.cn","itciraos.cn"],"iss":"TrustAsia TLS RSA CA","li":26504668,"naf":"2022-07-15T23:59:59Z","nbf":"2021-07-16T00:00:00Z","sub":"CN=www.itciraos.cn","t":"cert"}
{"cn":"cloud.it-wy.cn","dns":["cloud.it-wy.cn"],"iss":"Encryption Everywhere DV TLS CA - G1","li":26504669,"naf":"2022-05-06T23:59:59Z","nbf":"2021-05-06T00:00:00Z","sub":"CN=cloud.it-wy.cn","t":"cert"}
`

func TestDBBootstrap(t *testing.T) {
	db := setupTestDB(t)
	defer teardownTestDB(t, db)

	// Check for expected tables
	tables := []string{
		"raw_certificates", "etl_flush_metrics", "root_domains", "subdomains",
		"certificates", "subdomain_certificates",
	}
	for _, table := range tables {
		var exists bool
		err := db.QueryRow(`
			SELECT EXISTS (
				SELECT 1 FROM information_schema.tables WHERE table_name = $1
			)`, table).Scan(&exists)
		require.NoError(t, err, "checking table: %s", table)
		require.True(t, exists, "expected table missing: %s", table)
	}

	// Check function
	var funcExists bool
	err := db.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM pg_proc WHERE proname = 'flush_raw_certificates'
		)`).Scan(&funcExists)
	require.NoError(t, err)
	require.True(t, funcExists, "ETL flush function missing")
}

func TestPartitionTables(t *testing.T) {
	db := setupTestDB(t)
	defer teardownTestDB(t, db)

	rows, err := db.Query(`
		SELECT tablename FROM pg_tables
		WHERE tablename LIKE 'certificates_%'
		ORDER BY tablename LIMIT 1
	`)
	require.NoError(t, err)
	defer rows.Close()
	found := false
	for rows.Next() {
		found = true
	}
	require.True(t, found, "at least one certificates_* partition should exist")
}

func TestInsertBatch(t *testing.T) {
	db := setupTestDB(t)
	defer teardownTestDB(t, db)

	cert := extractor.CertFieldsExtractorOutput{
		CommonName:         "www.example.com",
		DNSNames:           []string{"www.example.com"},
		OrganizationalUnit: []string{"IT"},
		Organization:       []string{"ExampleCorp"},
		Locality:           []string{"Mountain View"},
		Country:            []string{"US"},
		NotBefore:          time.Now().Add(-1 * time.Hour),
		NotAfter:           time.Now().Add(365 * 24 * time.Hour),
		Subject:            "CN=www.example.com,O=ExampleCorp",
		LogIndex:           42,
	}

	metrics := NewSlurploadMetrics()
	metrics.Start()

	err := insertBatch(
		context.Background(), db,
		[]extractor.CertFieldsExtractorOutput{cert},
		0, metrics)
	require.NoError(t, err)

	require.NoError(t, FlushNow(db))

	// Query for inserted certificate
	var cn string
	err = db.QueryRow(`SELECT cn FROM certificates WHERE cn = $1`, "www.example.com").Scan(&cn)
	require.NoError(t, err)
	require.Equal(t, "www.example.com", cn)
}

func TestETLFlush_Basic(t *testing.T) {
	db := setupTestDB(t)
	defer teardownTestDB(t, db)

	// Insert N records into raw_certificates (simulate ingest)
	const N = 5
	for i := 0; i < N; i++ {
		_, err := db.Exec(`
			INSERT INTO raw_certificates (
				cert_type, common_name, dns_names, fqdn, not_before, not_after, subject, log_index
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8
			)`,
			"cert",
			fmt.Sprintf("flush-test-%d.com", i),
			pq.Array([]string{fmt.Sprintf("flush-test-%d.com", i)}),
			fmt.Sprintf("flush-test-%d.com", i),
			time.Now().Add(-24*time.Hour),
			time.Now().Add(24*time.Hour),
			fmt.Sprintf("CN=flush-test-%d.com", i),
			100+i,
		)
		require.NoError(t, err)
	}

	// Call FlushIfNeeded with threshold=1 to force a flush
	cfg := &SlurploadConfig{}
	cfg.Processing.FlushThreshold = 1
	cfg.Processing.FlushLimit = 1000

	metrics := NewSlurploadMetrics()
	metrics.Start()

	FlushIfNeeded(db, cfg, metrics)

	// Assert records are now in certificates (not just raw_certificates)
	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM certificates WHERE cn LIKE 'flush-test-%'`).Scan(&count))
	require.Equal(t, N, count, "Expected %d certs after ETL flush", N)

	// Assert raw_certificates table is now empty for those rows
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM raw_certificates WHERE common_name LIKE 'flush-test-%'`).Scan(&count))
	require.Equal(t, 0, count, "raw_certificates should be empty for flushed rows")

	// Assert flush metrics were written
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM etl_flush_metrics WHERE status = 'success'`).Scan(&count))
	require.True(t, count > 0, "should have at least one successful ETL metrics entry")
}

func TestRunFlusher_Interval(t *testing.T) {
	db := setupTestDB(t)
	defer teardownTestDB(t, db)
	cfg := &SlurploadConfig{}
	cfg.Processing.FlushThreshold = 1
	cfg.Processing.FlushLimit = 1000
	cfg.Processing.FlushInterval = 100 * time.Millisecond

	metrics := NewSlurploadMetrics()
	metrics.Start()

	// Insert a record
	_, err := db.Exec(`
		INSERT INTO raw_certificates (cert_type, common_name, dns_names, fqdn, not_before, not_after, subject, log_index)
		VALUES ('cert', 'interval-flush.com', $1, 'interval-flush.com', $2, $3, 'CN=interval-flush.com', 555)`,
		pq.Array([]string{"interval-flush.com"}),
		time.Now().Add(-24*time.Hour),
		time.Now().Add(24*time.Hour),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go RunFlusher(ctx, db, cfg, metrics)

	// Wait a bit to let flush run
	time.Sleep(300 * time.Millisecond)

	// Check it was flushed
	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM certificates WHERE cn = 'interval-flush.com'`).Scan(&count))
	require.Equal(t, 1, count)
}

func TestETLFlush_BatchFlushLimit(t *testing.T) {
	db := setupTestDB(t)
	defer teardownTestDB(t, db)

	const N = 8
	const flushLimit = 5

	// Insert N records into raw_certificates
	for i := 0; i < N; i++ {
		_, err := db.Exec(`
			INSERT INTO raw_certificates (
				cert_type, common_name, dns_names, fqdn, not_before, not_after, subject, log_index
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8
			)`,
			"cert",
			fmt.Sprintf("limit-test-%d.com", i),
			pq.Array([]string{fmt.Sprintf("limit-test-%d.com", i)}),
			fmt.Sprintf("limit-test-%d.com", i),
			time.Now().Add(-24*time.Hour),
			time.Now().Add(24*time.Hour),
			fmt.Sprintf("CN=limit-test-%d.com", i),
			200+i,
		)
		require.NoError(t, err)
	}

	cfg := &SlurploadConfig{}
	cfg.Processing.FlushThreshold = 1 // Force flush always
	cfg.Processing.FlushLimit = flushLimit

	metrics := NewSlurploadMetrics()
	metrics.Start()

	// First flush (should only move flushLimit rows)
	FlushIfNeeded(db, cfg, metrics)

	var processed, left int
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM certificates WHERE cn LIKE 'limit-test-%'`,
	).Scan(&processed))
	require.Equal(t, flushLimit, processed, "should process only flushLimit rows")

	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM raw_certificates WHERE common_name LIKE 'limit-test-%'`,
	).Scan(&left))
	require.Equal(t, N-flushLimit, left, "should leave the rest for next flush")

	// Second flush (should process remaining rows)
	FlushIfNeeded(db, cfg, metrics)

	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM certificates WHERE cn LIKE 'limit-test-%'`,
	).Scan(&processed))
	require.Equal(t, N, processed, "should have processed all rows after two flushes")

	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM raw_certificates WHERE common_name LIKE 'limit-test-%'`,
	).Scan(&left))
	require.Equal(t, 0, left, "all raw_certificates should be flushed now")
}

func TestETLFlush_MetricsTable(t *testing.T) {
	db := setupTestDB(t)
	defer teardownTestDB(t, db)

	const N = 3
	const flushLimit = 2

	// Insert N records into raw_certificates
	for i := 0; i < N; i++ {
		_, err := db.Exec(`
			INSERT INTO raw_certificates (
				cert_type, common_name, dns_names, fqdn, not_before, not_after, subject, log_index
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8
			)`,
			"cert",
			fmt.Sprintf("metrics-test-%d.com", i),
			pq.Array([]string{fmt.Sprintf("metrics-test-%d.com", i)}),
			fmt.Sprintf("metrics-test-%d.com", i),
			time.Now().Add(-24*time.Hour),
			time.Now().Add(24*time.Hour),
			fmt.Sprintf("CN=metrics-test-%d.com", i),
			300+i,
		)
		require.NoError(t, err)
	}

	cfg := &SlurploadConfig{}
	cfg.Processing.FlushThreshold = 1
	cfg.Processing.FlushLimit = flushLimit

	metrics := NewSlurploadMetrics()
	metrics.Start()

	// First flush (processes flushLimit)
	FlushIfNeeded(db, cfg, metrics)

	var loaded, inserted, deduped int64
	var status, flushType string

	// Get the latest etl_flush_metrics row
	row := db.QueryRow(
		`SELECT rows_loaded, rows_inserted, rows_deduped, status, flush_type
		 FROM etl_flush_metrics
		 ORDER BY id DESC LIMIT 1`,
	)
	require.NoError(t, row.Scan(&loaded, &inserted, &deduped, &status, &flushType))

	require.Equal(t, int64(flushLimit), loaded, "flush should load flushLimit rows")
	require.Equal(t, int64(flushLimit), inserted, "flush should insert flushLimit rows")
	require.Equal(t, int64(0), deduped, "should be no deduplication in first flush")
	require.Equal(t, "success", status)
	require.Equal(t, "batch", flushType)

	// Second flush (processes remaining N - flushLimit)
	FlushIfNeeded(db, cfg, metrics)
	row = db.QueryRow(
		`SELECT rows_loaded, rows_inserted, rows_deduped, status, flush_type
		 FROM etl_flush_metrics
		 ORDER BY id DESC LIMIT 1`,
	)
	require.NoError(t, row.Scan(&loaded, &inserted, &deduped, &status, &flushType))

	require.Equal(t, int64(N-flushLimit), loaded, "second flush should load remaining rows")
	require.Equal(t, int64(N-flushLimit), inserted)
	require.Equal(t, int64(0), deduped)
	require.Equal(t, "success", status)
}

func TestProcessFileJob_Plain_Gz_Bz2(t *testing.T) {
	dir := t.TempDir()
	for _, ext := range []string{".jsonl", ".jsonl.gz", ".jsonl.bz2"} {
		t.Run(ext, func(t *testing.T) {
			db := setupTestDB(t)
			defer teardownTestDB(t, db)
			path := writeTestFile(t, dir, ext, testData)
			metrics := NewSlurploadMetrics()
			metrics.Start()
			job := InsertJob{Name: filepath.Base(path), Path: path}
			err := processFileJob(context.Background(), db, job, 10, 0, metrics)
			require.NoError(t, err)

			require.NoError(t, FlushNow(db))

			var count int
			require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM certificates`).Scan(&count))
			require.Equal(t, 1, count)
		})
	}
}

const testData string = `{"cn":"www.example.com","dns":["www.example.com"],"ou":["IT"],"o":["ExampleCorp"],"l":["Mountain View"],"c":["US"],"sub":"CN=www.example.com,O=ExampleCorp","nbf":"2023-01-01T00:00:00Z","naf":"2024-01-01T00:00:00Z","en":1}`

func TestHTTPEndpoint(t *testing.T) {
	db := setupTestDB(t)
	defer teardownTestDB(t, db)

	inboxDir := t.TempDir()
	stop := make(chan struct{})
	jobs := make(chan InsertJob, 2)

	cfg := NewWatcherConfig(inboxDir, "", []string{"*.jsonl"}, 50*time.Millisecond)
	go StartInboxWatcher(cfg, jobs, stop)

	srv := httptest.NewUnstartedServer(uploadHandler(inboxDir))
	srv.Start()
	defer srv.Close()

	// POST test data (plain JSONL)
	resp, err := http.Post(srv.URL+"/upload", "application/json", bytes.NewReader([]byte(testData)))
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	// Wait for watcher to see and enqueue the job
	var job InsertJob
	select {
	case job = <-jobs:
	case <-time.After(3 * time.Second):
		close(stop)
		t.Fatal("timed out waiting for watcher to enqueue job")
	}
	close(stop) // stop watcher

	// File should exist
	_, err = os.Stat(job.Path)
	require.NoError(t, err)

	// Process the file
	metrics := NewSlurploadMetrics()
	metrics.Start()
	err = processFileJob(context.Background(), db, job, 10, 0, metrics)
	require.NoError(t, FlushNow(db))
	require.NoError(t, err)

	// Assert DB content
	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM certificates`).Scan(&count))
	require.Equal(t, 1, count)
}

func TestUploadHandler_Methods(t *testing.T) {
	inboxDir := t.TempDir()
	handler := uploadHandler(inboxDir)

	cases := []struct {
		method     string
		wantStatus int
	}{
		{"POST", http.StatusNoContent},
		{"PUT", http.StatusNoContent},
		{"GET", http.StatusMethodNotAllowed},
		{"DELETE", http.StatusMethodNotAllowed},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(tc.method, "/upload", bytes.NewReader([]byte(testData)))
		w := httptest.NewRecorder()
		handler(w, req)
		resp := w.Result()
		require.Equal(t, tc.wantStatus, resp.StatusCode)
	}
}

func TestInboxWatcher_Workers_E2E(t *testing.T) {
	db := setupTestDB(t)
	defer teardownTestDB(t, db)

	inboxDir := t.TempDir()
	writeTestFile(t, inboxDir, ".jsonl", testData+"\n")

	jobs := make(chan InsertJob, 2)
	stop := make(chan struct{})

	cfg := NewWatcherConfig(inboxDir, "", []string{"*.jsonl"}, 100*time.Millisecond)

	// Start watcher
	go StartInboxWatcher(cfg, jobs, stop)

	metrics := NewSlurploadMetrics()
	metrics.Start()

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				_ = processFileJob(context.Background(), db, job, 10, 0, metrics)
			}
		}()
	}

	// Shutdown
	time.Sleep(200 * time.Millisecond) // let watcher find files
	close(stop)                        // stop watcher
	time.Sleep(100 * time.Millisecond) // let jobs channel fill
	close(jobs)                        // let workers drain jobs
	wg.Wait()

	require.NoError(t, FlushNow(db))
	// Assert DB content
	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM certificates`).Scan(&count))
	require.Equal(t, 1, count)
}

func TestHTTPEndpoint_Compressed(t *testing.T) {
	inboxDir := t.TempDir()
	stop := make(chan struct{})
	jobs := make(chan InsertJob, 2)
	cfg := NewWatcherConfig(inboxDir, "", []string{"*.jsonl", "*.jsonl.gz", "*.jsonl.bz2"}, 50*time.Millisecond)
	go StartInboxWatcher(cfg, jobs, stop)

	srv := httptest.NewUnstartedServer(uploadHandler(inboxDir))
	srv.Start()
	defer srv.Close()

	type testCase struct {
		name     string
		content  []byte
		ct       string
		encoding string
		ext      string
	}
	tests := []testCase{
		{
			name:     "plain",
			content:  []byte(testData),
			ct:       "application/json",
			encoding: "",
			ext:      ".jsonl",
		},
		{
			name:     "gzip",
			content:  compressGzip([]byte(testData)),
			ct:       "application/json",
			encoding: "gzip",
			ext:      ".jsonl.gz",
		},
		{
			name:     "bzip2",
			content:  compressBzip2([]byte(testData)),
			ct:       "application/json",
			encoding: "bzip2",
			ext:      ".jsonl.bz2",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := setupTestDB(t)
			defer teardownTestDB(t, db)

			req, err := http.NewRequest("POST", srv.URL+"/upload", bytes.NewReader(tc.content))
			require.NoError(t, err)
			req.Header.Set("Content-Type", tc.ct)
			if tc.encoding != "" {
				req.Header.Set("Content-Encoding", tc.encoding)
			}
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			require.Equal(t, http.StatusNoContent, resp.StatusCode)

			var job InsertJob
			select {
			case job = <-jobs:
			case <-time.After(3 * time.Second):
				close(stop)
				t.Fatalf("timed out waiting for watcher to enqueue job for %s", tc.name)
			}

			metrics := NewSlurploadMetrics()
			metrics.Start()

			err = processFileJob(context.Background(), db, job, 10, 0, metrics)
			require.NoError(t, err)

			require.NoError(t, FlushNow(db))

			var count int
			require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM certificates`).Scan(&count))
			require.Equal(t, 1, count)
		})
	}

	close(stop)
}

func TestWatcherMovesToDoneDir(t *testing.T) {
	db := setupTestDB(t)
	defer teardownTestDB(t, db)

	inboxDir := t.TempDir()
	doneDir := t.TempDir()

	// Place a file in the inbox
	_ = writeTestFile(t, inboxDir, ".jsonl", testData+"\n")

	jobs := make(chan InsertJob, 1)
	stop := make(chan struct{})

	cfg := NewWatcherConfig(inboxDir, doneDir, []string{"*.jsonl"}, 200*time.Millisecond)

	go StartInboxWatcher(cfg, jobs, stop)

	// Get the job
	var job InsertJob
	select {
	case job = <-jobs:
	case <-time.After(2 * time.Second):
		close(stop)
		t.Fatal("timed out waiting for watcher")
	}
	close(stop)

	// Run the worker
	metrics := NewSlurploadMetrics()
	metrics.Start()
	err := processFileJob(context.Background(), db, job, 10, 0, metrics)
	require.NoError(t, err)

	// Move file (simulate worker cleanup)
	dest := filepath.Join(doneDir, filepath.Base(job.Path))
	err = os.Rename(job.Path, dest)
	require.NoError(t, err)

	// File should not exist in inbox, but must exist in doneDir
	_, err = os.Stat(job.Path)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(dest)
	require.NoError(t, err)
}

func TestLoadConfig_YAML(t *testing.T) {
	// Create a temp YAML file
	yamlContent := `
database:
  host: "localhost"
  port: 5432
  username: "test"
  password: "s3cr3t"
  database: "certs"
  ssl_mode: "disable"
  max_conns: 10
  batch_size: 50
server:
  listen_addr: ":8081"
processing:
  inbox_dir: "/tmp/inbox"
  done_dir: "/tmp/done"
  inbox_patterns: "*.jsonl,*.gz"
  inbox_poll: 1s
  enable_watcher: true
metrics:
  log_stat_every: 17
`
	f, err := os.CreateTemp("", "slurpload-config-*.yaml")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	_, err = f.Write([]byte(yamlContent))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	cfg, err := loadConfig(f.Name())
	require.NoError(t, err)

	// Check all fields
	require.Equal(t, "localhost", cfg.Database.Host)
	require.Equal(t, 5432, cfg.Database.Port)
	require.Equal(t, "test", cfg.Database.Username)
	require.Equal(t, "s3cr3t", cfg.Database.Password)
	require.Equal(t, "certs", cfg.Database.DatabaseName)
	require.Equal(t, "disable", cfg.Database.SSLMode)
	require.Equal(t, 10, cfg.Database.MaxConns)
	require.Equal(t, 50, cfg.Database.BatchSize)
	require.Equal(t, ":8081", cfg.Server.ListenAddr)
	require.Equal(t, "/tmp/inbox", cfg.Processing.InboxDir)
	require.Equal(t, "/tmp/done", cfg.Processing.DoneDir)
	require.Equal(t, "*.jsonl,*.gz", cfg.Processing.InboxPatterns)
	require.Equal(t, true, cfg.Processing.EnableWatcher)
	require.Equal(t, int64(17), cfg.Metrics.LogStatEvery)
	require.Equal(t, 1*time.Second, cfg.Processing.InboxPollInterval)
}

func TestLoadConfig_Validation(t *testing.T) {
	// Minimal config with missing required fields
	yamlContent := `
database:
  port: 5432
`
	f, err := os.CreateTemp("", "slurpload-config-*.yaml")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	_, err = f.Write([]byte(yamlContent))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = loadConfig(f.Name())
	require.Error(t, err)
	require.Contains(t, err.Error(), "database.host")
}

func TestMetricsHandler(t *testing.T) {
	metrics := NewSlurploadMetrics()
	metrics.Start()
	metrics.IncProcessed()
	metrics.IncFailed()

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()

	handler := metricsHandler(metrics)
	handler(w, req)

	resp := w.Result()
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	// Check content type and body
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	require.Contains(t, string(body), `"processed":1`)
	require.Contains(t, string(body), `"failed":1`)
}
