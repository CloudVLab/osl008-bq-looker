view: market_tec_query_2 {
  derived_table: {
    sql:
        WITH
          undly_instrument AS
        (
        SELECT distinct
               inst.run_date as cycle_date,
               inst.exch_mic,
               inst.glbx_alias as glbx_instrument,
               inst.glbx_sym as glbx_prod_cd,
               case when (inst.put_call_ind = 'C' or inst.put_call_ind = 'P') then 'OPT' else 'FUT' end as prod_type,
               inst.put_call_ind,
               inst.strike_px,
               inst.contract_period,
               case when (inst.put_call_ind = 'C' or inst.put_call_ind = 'P')  then und_inst.glbx_sym else inst.glbx_sym end as undly_glbx_prod_cd,
               case when (inst.put_call_ind = 'C' or inst.put_call_ind = 'P') then und_inst.glbx_alias else inst.glbx_alias end as undly_glbx_instrument
            FROM cloud-training-demos.fsi_customer_demo_cme.instrument_fno inst
            LEFT JOIN cloud-training-demos.fsi_customer_demo_cme.option_series_fno opt
              ON inst.run_date = opt.run_date AND inst.Instr_guid_int = opt.Instr_guid_int
            LEFT JOIN cloud-training-demos.fsi_customer_demo_cme.instrument_fno und_inst
              ON inst.run_date = und_inst.run_date AND opt.underlying_instr_guid_int = und_inst.Instr_guid_int
          where
          {% condition run_date %} inst.run_date {% endcondition %}
          AND {% condition exchange %} inst.exch_mic {% endcondition %}
          AND ({% condition undly_glbx_prod_cd %} und_inst.glbx_sym {% endcondition %} OR {% condition undly_glbx_prod_cd %} inst.glbx_sym {% endcondition %})
    ),
    ob_change_calc as
    (
        SELECT ob.cycle_date,
        clr_sym as prod_cd,
        glbx_sym AS instrument,
        exchange_mic AS exch_mic,
        security_type AS product_type,
        glbx_sym as inst_sym,
        implied_book_ind,
        uin.undly_glbx_prod_cd,
        uin.undly_glbx_instrument,
      

      extract(HOUR from datetime(transaction_ts)) as tm_increment_value_utc,
      extract(HOUR from datetime(transaction_ts, 'America/Chicago')) as tm_increment_value_cst,
      
      
      CASE WHEN COALESCE(LAG(bid_level_1_qty) OVER (PARTITION BY ob.cycle_date, glbx_security_id ORDER BY rpt_seq_nbr) , 0) = bid_level_1_qty
      AND COALESCE(LAG(ask_level_1_qty) OVER (PARTITION BY ob.cycle_date, glbx_security_id ORDER BY rpt_seq_nbr) , 0) = ask_level_1_qty
      AND COALESCE(LAG(bid_level_1_px) OVER (PARTITION BY ob.cycle_date, glbx_security_id ORDER BY rpt_seq_nbr) , 0) = bid_level_1_px
      AND COALESCE(LAG(ask_level_1_px) OVER (PARTITION BY ob.cycle_date, glbx_security_id ORDER BY rpt_seq_nbr) , 0) = ask_level_1_px
      THEN 0
      ELSE 1
      END AS ord_book_upd
      FROM cloud-training-demos.fsi_customer_demo_cme.orderbook_fno  ob
      inner join undly_instrument uin
      on ob.exchange_mic = uin.exch_mic and ob.glbx_sym = uin.glbx_instrument
      WHERE
      {% condition run_date %} ob.cycle_date {% endcondition %}
      AND {% condition exchange %} ob.exchange_mic {% endcondition %}
      AND NOT (glbx_sym LIKE "%-%" OR glbx_sym LIKE "%:%")
      AND security_type in ("FUT", "OPT")
      AND {% condition undly_glbx_prod_cd %} uin.undly_glbx_prod_cd {% endcondition %}
      AND implied_book_ind = 'N'
      ),
      tm_increment_stats as
      (
      SELECT  cycle_date,
      undly_glbx_prod_cd,
      product_type,
      tm_increment_value_cst,
      sum(ord_book_upd) as num_ob_updates
      FROM ob_change_calc occ
      group by cycle_date,
      undly_glbx_prod_cd,
      product_type,
      tm_increment_value_cst
      )
      select *
      from tm_increment_stats
      ;;
  }
  
  #parameter: parameter_tm_increment {
  #  type: unquoted
  #  label: "Time Increment"
  #  default_value: "MINUTE"
  #  allowed_value: {label: "Minute" value: "MINUTE"}
  #  allowed_value: {label: "Hour" value: "HOUR"}
  #  allowed_value: {label: "Second" value: "SECOND"}
  #}
  
  dimension: cycle_date_hidden_dimension{
    type: string
    label: "Cycle Date HIDDEN DIMENSION"
    description: "THIS IS A HIDDEN FILED AND USED BY THE CYCLE DATE FILTER"
    convert_tz: no
    datatype: date
    can_filter: yes
    hidden: yes
    sql: ${TABLE}.cycle_date
      ;;
  }
  
  filter: run_date  {
    label: "Cycle Date Filter"
    type: date
    convert_tz: no
    datatype: date
    sql: {% condition %} ${cycle_date_hidden_dimension} {% endcondition %};;
  }
  
  filter: exchange {
    label: "Exchange"
    type: string
    #suggest_dimension: inst.exch_mic
  }
  
  filter: undly_glbx_prod_cd {
    label: "Product Code"
    type: string
    #suggest_dimension: undly_prod_code
  }
  
  filter: hour {
    label: "Hour"
    type: number
  }
  
  filter: minute {
    label: "Minute"
    type: number
  }
  
  dimension: cyc_dt {
    type: date
    label: "Cycle Date"
    datatype: date
    sql: ${TABLE}.cycle_date ;;
  }
  
  dimension: undly_prod_code {
    type: string
    label: "Underlying Product Code"
    sql: ${TABLE}.undly_glbx_prod_cd ;;
  }
  
  dimension: product_type {
    type: string
    label: "Product Type"
    sql: ${TABLE}.product_type ;;
  }
  
  #dimension: tm_increment {
  #  type: string
  #  label: "Time Increment"
  #  sql: ${TABLE}.tm_increment ;;
  #}
  
  dimension: tm_increment_cst {
    type: string
    label: "Time Increment CST"
    sql: ${TABLE}.tm_increment_value_cst ;;
  }
  
  measure: num_ob_updates {
    type: number
    label: "Total Order Book Updates"
    sql: sum(${TABLE}.num_ob_updates) ;;
  }
  
  
}
