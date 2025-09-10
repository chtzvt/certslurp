provider "azurerm" {
  features {}
  subscription_id                 = local.subscription_id
  resource_provider_registrations = "none"
}

locals {
  subscription_id = var.subscription_id
  location        = var.location
  resource_group  = var.resource_group

  public_ip_count = var.public_ip_count

  ssh_public_key = var.ssh_public_key
  my_home_ip     = chomp(data.http.wanip.response_body)
}

variable "subscription_id" {
  description = "Azure subscription id"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
}

variable "resource_group" {
  description = "resource group name"
  type        = string
}

variable "public_ip_count" {
  description = "number of public IPs to deploy for scraping traffic/egress"
  type        = number
  default     = 32
}

variable "ssh_public_key" {
  description = "your ssh public key"
  type        = string
}

data "http" "wanip" {
  url = "http://whatismyip.akamai.com"
}

output "egressrouter_mgmt_ip" {
  value = azurerm_public_ip.egressrouter_mgmt.ip_address
}

output "egressrouter_public_ips" {
  value = [for ip in azurerm_public_ip.egressrouter_wan : ip.ip_address]
}

output "my_home_ip" {
  value = local.my_home_ip
}

resource "azurerm_resource_group" "main" {
  name     = local.resource_group
  location = local.location
}

resource "random_integer" "suffix" {
  min = 1000
  max = 9999
}

resource "azurerm_container_registry" "certslurp" {
  name                = "certslurp${random_integer.suffix.result}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = "Standard"
  admin_enabled       = true
}

resource "azurerm_virtual_network" "main" {
  name                = "certslurp-vnet"
  location            = local.location
  resource_group_name = azurerm_resource_group.main.name
  address_space       = ["10.0.0.0/16"]
}

resource "azurerm_subnet" "aks" {
  name                 = "aks-subnet"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = ["10.0.0.0/22"]

  service_endpoints = ["Microsoft.Storage"]
}

resource "azurerm_network_security_group" "egressrouter" {
  name                = "egressrouter-nsg"
  location            = local.location
  resource_group_name = azurerm_resource_group.main.name

  security_rule {
    name                       = "AllowTrafficFromAKS"
    priority                   = 110
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "*"
    source_address_prefix      = azurerm_subnet.aks.address_prefixes[0]
    destination_port_range     = "*"
    destination_address_prefix = "*"
    source_port_range          = "*"
  }

  security_rule {
    name                       = "AllowSSHFromHome"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_address_prefix      = "${local.my_home_ip}/32"
    destination_port_range     = "22"
    destination_address_prefix = "*"
    source_port_range          = "*"
  }

  security_rule {
    name                       = "DenyAllInbound"
    priority                   = 4000
    direction                  = "Inbound"
    access                     = "Deny"
    protocol                   = "*"
    source_address_prefix      = "*"
    destination_port_range     = "*"
    destination_address_prefix = "*"
    source_port_range          = "*"
  }
}

resource "azurerm_route_table" "egressrouter" {
  name                = "egressrouter-rt"
  location            = local.location
  resource_group_name = local.resource_group
}

resource "azurerm_route" "default_egress" {
  name                   = "default-egress"
  resource_group_name    = local.resource_group
  route_table_name       = azurerm_route_table.egressrouter.name
  address_prefix         = "0.0.0.0/0"
  next_hop_type          = "VirtualAppliance"
  next_hop_in_ip_address = "10.0.4.4"
}

resource "azurerm_route" "private_10" {
  name                = "private-10"
  resource_group_name = local.resource_group
  route_table_name    = azurerm_route_table.egressrouter.name
  address_prefix      = "10.0.0.0/8"
  next_hop_type       = "VnetLocal"
}

resource "azurerm_route" "private_172" {
  name                = "private-172"
  resource_group_name = local.resource_group
  route_table_name    = azurerm_route_table.egressrouter.name
  address_prefix      = "172.16.0.0/12"
  next_hop_type       = "VnetLocal"
}

resource "azurerm_route" "private_192" {
  name                = "private-192"
  resource_group_name = local.resource_group
  route_table_name    = azurerm_route_table.egressrouter.name
  address_prefix      = "192.168.0.0/16"
  next_hop_type       = "VnetLocal"
}

resource "azurerm_subnet_route_table_association" "aks" {
  subnet_id      = azurerm_subnet.aks.id
  route_table_id = azurerm_route_table.egressrouter.id
}

# AKS Cluster
resource "azurerm_kubernetes_cluster" "main" {
  depends_on = [azurerm_linux_virtual_machine.egressrouter, azurerm_container_registry.certslurp]

  name                = "certslurp-aks-${random_integer.suffix.result}"
  location            = local.location
  resource_group_name = azurerm_resource_group.main.name
  dns_prefix          = "certslurp"

  default_node_pool {
    name                        = "nodepool1"
    temporary_name_for_rotation = "certslurptmp"

    # Note: MUST have at least as many nodes as etd pods
    node_count     = 3
    max_pods       = 80
    vm_size        = "Standard_B16als_v2"
    vnet_subnet_id = azurerm_subnet.aks.id
  }

  identity {
    type = "SystemAssigned"
  }

  network_profile {
    network_plugin = "azure"
    outbound_type  = "userDefinedRouting"
    service_cidr   = "10.96.0.0/16"
    dns_service_ip = "10.96.0.10"
  }
}

#resource "azurerm_role_assignment" "aks_to_acr" {
#  scope                = azurerm_container_registry.certslurp.id
#  role_definition_name = "AcrPull"
#  principal_id         = azurerm_kubernetes_cluster.main.kubelet_identity[0].object_id
#}

# Egress Router VM
resource "azurerm_public_ip" "egressrouter_wan" {
  count               = local.public_ip_count
  name                = "egressrouter-pip-${count.index}"
  location            = local.location
  resource_group_name = azurerm_resource_group.main.name
  allocation_method   = "Static"
  sku                 = "Standard"
}

resource "azurerm_network_interface" "egressrouter_wan" {
  name                           = "egressrouter-nic-wan"
  location                       = local.location
  resource_group_name            = azurerm_resource_group.main.name
  accelerated_networking_enabled = true
  ip_forwarding_enabled          = true

  dynamic "ip_configuration" {
    for_each = azurerm_public_ip.egressrouter_wan
    content {
      name                          = "internal-${ip_configuration.key}"
      subnet_id                     = azurerm_subnet.egressrouter_wan.id
      private_ip_address_allocation = "Dynamic"
      public_ip_address_id          = ip_configuration.value.id
      primary                       = ip_configuration.key == 0 ? true : false
    }
  }
}

resource "azurerm_public_ip" "egressrouter_mgmt" {
  name                = "egressrouter-mgmt"
  location            = local.location
  resource_group_name = azurerm_resource_group.main.name
  allocation_method   = "Static"
  sku                 = "Standard"
}

resource "azurerm_network_interface" "egressrouter_mgmt" {
  name                           = "egressrouter-nic-mgmt"
  location                       = local.location
  resource_group_name            = azurerm_resource_group.main.name
  accelerated_networking_enabled = true
  ip_forwarding_enabled          = true

  ip_configuration {
    name                          = "mgmt"
    subnet_id                     = azurerm_subnet.egressrouter.id
    private_ip_address_allocation = "Static"
    private_ip_address            = "10.0.4.4"
    public_ip_address_id          = azurerm_public_ip.egressrouter_mgmt.id
    primary                       = true
  }
}

resource "azurerm_network_interface_security_group_association" "egressrouter_mgmt" {
  network_interface_id      = azurerm_network_interface.egressrouter_mgmt.id
  network_security_group_id = azurerm_network_security_group.egressrouter.id
}

resource "azurerm_network_interface_security_group_association" "egressrouter_wan" {
  network_interface_id      = azurerm_network_interface.egressrouter_wan.id
  network_security_group_id = azurerm_network_security_group.egressrouter.id
}


resource "azurerm_subnet" "egressrouter" {
  name                 = "egressrouter-subnet"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = ["10.0.4.0/24"]

  service_endpoints = ["Microsoft.Storage"]
}

resource "azurerm_subnet" "egressrouter_wan" {
  name                 = "egressrouter-wan-subnet"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = ["10.0.5.0/24"]
  service_endpoints    = ["Microsoft.Storage"]
}

resource "azurerm_linux_virtual_machine" "egressrouter" {
  name                  = "egressrouter-vm"
  resource_group_name   = azurerm_resource_group.main.name
  location              = local.location
  size                  = "Standard_B8als_v2"
  admin_username        = "azureuser"
  network_interface_ids = [azurerm_network_interface.egressrouter_wan.id, azurerm_network_interface.egressrouter_mgmt.id]

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "StandardSSD_LRS"
    disk_size_gb         = 64
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "ubuntu-24_04-lts"
    sku       = "server"
    version   = "latest"
  }

  admin_ssh_key {
    username   = "azureuser"
    public_key = local.ssh_public_key
  }

  custom_data = base64encode(file("${path.module}/init_scripts/egressrouter_init.sh.tpl"))
}
