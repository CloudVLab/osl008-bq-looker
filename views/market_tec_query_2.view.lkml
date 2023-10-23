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
            LEFT JOIN cloud-training-demos.fsi_customer_demo_cme.underlying_instrument_fno und_inst
              ON inst.run_date = und_inst.run_date AND opt.underlying_instr_guid_int = und_inst.Instr_guid_int
          where
          {% condition run_date %} inst.run_date {% endcondition %}
          AND {% condition exchange %} inst.exch_mic {% endcondition %}
          AND ({% condition symbol %} und_inst.glbx_sym {% endcondition %} OR {% condition symbol %} inst.glbx_sym {% endcondition %})
    ),
    ob_change_calc as
    (
      SELECT cyc_dt AS cycle_date,
        inst_prod_cd as prod_cd,
        inst_sym AS instrument,
        inst_exch_mrkt_id AS exch_mic,
        inst_prod_typ AS product_type,
        inst_sym,
        impd_ind,
        uin.undly_glbx_prod_cd,
        uin.undly_glbx_instrument,
        
            {% if parameter_tm_increment._parameter_value == 'HOUR' %}
              extract(HOUR from datetime(txn_tmsp))
            {% elsif parameter_tm_increment._parameter_value  == 'MINUTE' %}
              extract(MINUTE from datetime(txn_tmsp))
           {% else %}
              extract(SECOND from datetime(txn_tmsp))  {% endif %} as tm_increment,

      extract(HOUR from datetime(txn_tmsp)) as tm_increment_value_utc,
      extract(HOUR from datetime(txn_tmsp, 'America/Chicago')) as tm_increment_value_cst,
     

      CASE WHEN COALESCE(LAG(bid_lvl_1_qty) OVER (PARTITION BY cyc_dt, inst_isin_id ORDER BY seq_nbr) , 0) = bid_lvl_1_qty
          AND COALESCE(LAG(ask_lvl_1_qty) OVER (PARTITION BY cyc_dt, inst_isin_id ORDER BY seq_nbr) , 0) = ask_lvl_1_qty
          AND COALESCE(LAG(bid_lvl_1_px_dec) OVER (PARTITION BY cyc_dt, inst_isin_id ORDER BY seq_nbr) , 0) = bid_lvl_1_px_dec
          AND COALESCE(LAG(ask_lvl_1_px_dec) OVER (PARTITION BY cyc_dt, inst_isin_id ORDER BY seq_nbr) , 0) = ask_lvl_1_px_dec
      THEN 0
      ELSE 1
      END AS ord_book_upd
      FROM cloud-training-demos.fsi_customer_demo_cme.orderbook_fno  ob
      inner join undly_instrument uin
      on ob.inst_exch_mrkt_id = uin.exch_mic and ob.inst_sym = uin.glbx_instrument
      WHERE
      {% condition run_date %} cyc_dt {% endcondition %}
      AND {% condition exchange %} ob.inst_exch_mrkt_id {% endcondition %}
      AND NOT (inst_sym LIKE "%-%" OR inst_sym LIKE "%:%")
      AND inst_prod_typ in ("FUT", "OPT")
      AND {% condition symbol %} uin.undly_glbx_prod_cd {% endcondition %}
      AND impd_ind = 'N'
      AND
      {% if parameter_tm_increment._parameter_value  == 'MINUTE' %}
      {% condition hour %} hour {% endcondition %}
      {% elsif parameter_tm_increment._parameter_value  == 'SECOND' %}
      {% condition hour %} hour {% endcondition %} AND {% condition minute %} extract(MINUTE from datetime(txn_tmsp, 'America/Chicago')) {% endcondition %}
      {% else %}
      {% condition hour %} hour {% endcondition %}
      {% endif %}
      ),
      tm_increment_stats as
      (
      SELECT  cycle_date,
              undly_glbx_prod_cd,
              product_type,
              tm_increment,
              tm_increment_value_cst,
              sum(ord_book_upd) as num_ob_updates
      FROM ob_change_calc occ
      group by cycle_date,
      undly_glbx_prod_cd,
      product_type,
      tm_increment,
      tm_increment_value_cst
      )
      select *
      from tm_increment_stats
      ;;
  }

  parameter: parameter_tm_increment {
    type: unquoted
    label: "Time Increment"
    default_value: "MINUTE"
    allowed_value: {label: "Minute" value: "MINUTE"}
    allowed_value: {label: "Hour" value: "HOUR"}
    allowed_value: {label: "Second" value: "SECOND"}
  }

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

  filter: symbol {
    label: "Instrument Symbol"
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

  dimension: tm_increment {
    type: string
    label: "Time Increment"
    sql: ${TABLE}.tm_increment ;;
  }

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