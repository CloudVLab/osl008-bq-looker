project_name: "market_tech"

#New LookML Runtime provides faster LookML validation and content validation
new_lookml_runtime: yes

################ Constants ################

constant: DB_CONNECTION {
  value: "bigquery_public_data_looker"
  export: override_required
}

constant: PROJECT_CURATED_ZONE_EXT {
  value: "cloud-training-demos"
}

constant: DATA_SET {
  value: "fsi_customer_demo_cme"
}

constant: DATA_SET_REF {
  value: "fsi_customer_demo_cme"
}

constant: MKT_DATA_T_ORDERBOOK_GLOBEX_10_LVL_DEEP {
  value:"`@{PROJECT_CURATED_ZONE_EXT}.@{DATA_SET}.orderbook_fno`"
  export: none
}

constant: MKT_DATA_T_STATISTICS_GLOBEX {
  value:"`@{PROJECT_CURATED_ZONE_EXT}.@{DATA_SET}.statistics_fno`"
  export: none
}

constant: MKT_DATA_T_INSTRUMENT_FNO {
  value:"`@{PROJECT_CURATED_ZONE_EXT}.@{DATA_SET_REF}.instrument_fno`"
  export: none
}