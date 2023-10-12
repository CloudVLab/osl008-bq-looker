project_name: "market_tech"

#New LookML Runtime provides faster LookML validation and content validation
new_lookml_runtime: yes

################ Constants ################

constant: DB_CONNECTION {
  value: "bigquery_public_data_looker"
  export: override_required
}

constant: PROJECT_CURATED_ZONE_EXT {
  value: "prj-pr-curated-zone-ext-1595"
}

constant: DATA_SET {
  value: "marketdata"
}

constant: DATA_SET_REF {
  value: "refdata"
}

constant: MKT_DATA_T_ORDERBOOK_GLOBEX_10_LVL_DEEP {
  value:"`@{PROJECT_CURATED_ZONE_EXT}.@{DATA_SET}.t_orderbook_globex_10_lvl_deep`"
  export: none
}

constant: MKT_DATA_T_QUOTE_REQUEST_GLOBEX {
  value:"`@{PROJECT_CURATED_ZONE_EXT}.@{DATA_SET}.t_quote_request_globex`"
  export: none
}

constant: MKT_DATA_T_STATISTICS_GLOBEX {
  value:"`@{PROJECT_CURATED_ZONE_EXT}.@{DATA_SET}.t_statistics_globex`"
  export: none
}

constant: MKT_DATA_T_INSTRUMENT_FNO {
  value:"`@{PROJECT_CURATED_ZONE_EXT}.@{DATA_SET_REF}.t_instrument_fno`"
  export: none
}