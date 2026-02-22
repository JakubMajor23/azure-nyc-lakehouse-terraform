output "resource_group_name" {
  value = azurerm_resource_group.rg.name
}

output "datalake_name" {
  value = azurerm_storage_account.datalake.name
}

output "synapse_workspace_name" {
  value = azurerm_synapse_workspace.synapse.name
}

output "synapse_sql_endpoint" {
  value = azurerm_synapse_workspace.synapse.connectivity_endpoints["sqlOnDemand"]
}

output "synapse_studio_url" {
  value = "https://web.azuresynapse.net?workspace=/subscriptions/${data.azurerm_client_config.current.subscription_id}/resourceGroups/${azurerm_resource_group.rg.name}/providers/Microsoft.Synapse/workspaces/${azurerm_synapse_workspace.synapse.name}"
}

output "data_factory_name" {
  value = azurerm_data_factory.adf.name
}

output "key_vault_name" {
  value = azurerm_key_vault.kv.name
}
