resource "azurerm_synapse_workspace" "synapse" {
  name                                 = "syn-${local.name_prefix}-${local.suffix}"
  resource_group_name                  = azurerm_resource_group.rg.name
  location                             = azurerm_resource_group.rg.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.synapse.id

  sql_administrator_login          = var.sql_admin_login
  sql_administrator_login_password = var.sql_admin_password
  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}

resource "azurerm_synapse_firewall_rule" "allow_all" {
  name                 = "AllowAll"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  start_ip_address     = "0.0.0.0"
  end_ip_address       = "255.255.255.255"
}
