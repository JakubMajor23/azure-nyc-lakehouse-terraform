resource "azurerm_data_factory" "adf" {
  name                = "adf-${local.name_prefix}-${local.suffix}"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name


  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}
