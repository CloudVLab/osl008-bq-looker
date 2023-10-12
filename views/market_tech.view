view: market_tec {
  derived_table: {
    sql:
      WITH
            inst_details AS
            (
              SELECT run_date,
                exch_id,
                glbx_group_id,
                glbx_alias,
                glbx_sym,
                clr_sym,
                contract_period,
                put_call_ind,
                strike_px,
                first_trade_date,
                last_trade_date,
                final_settlement_date,
                DATE_DIFF(DATE(last_trade_date), DATE(first_trade_date), DAY) AS num_days_contract,
                DATE_DIFF(DATE(final_settlement_date),date({% date_end run_date %}), DAY) AS num_days_to_expiry
              FROM @{MKT_DATA_T_INSTRUMENT_FNO} inst
              WHERE {% condition run_date %} run_date {% endcondition %}
                AND {% condition symbol %} glbx_sym {% endcondition %}

      ),
      tot_vol AS
      (
      SELECT cyc_dt,
      inst_sym,
      inst_prod_cd,
      MAX(vol_qty) AS max_vol
      FROM @{MKT_DATA_T_STATISTICS_GLOBEX}
      WHERE {% condition run_date %} cyc_dt {% endcondition %}
      AND {% condition symbol %} inst_prod_cd {% endcondition %}

      GROUP BY cyc_dt,
      inst_sym,
      inst_prod_cd
      ),
      top_of_book_stats AS
      (
      SELECT cyc_dt,
      inst_exch_mrkt_id AS exch_id,
      inst_ast_sub_clas,
      inst_sym,
      inst_ctrt_prd_cd,
      inst_prod_cd,
      inst_prod_typ,
      (bid_lvl_1_px_dec / 100) AS bid_lvl_1_px_dec,
      (ask_lvl_1_px_dec / 100) AS ask_lvl_1_px_dec,
      bid_lvl_1_qty,
      ask_lvl_1_qty,
      --
      (ob.bid_lvl_1_qty + ob.ask_lvl_1_qty) / 2 AS avg_level_1_qty,
      ((ob.bid_lvl_1_px_dec / 100) + (ob.ask_lvl_1_px_dec / 100)) / 2 AS avg_level_1_px_dec,
      (((ob.bid_lvl_1_px_dec / 100) + (ob.ask_lvl_1_px_dec / 100)) / 2) * ((ob.bid_lvl_1_qty + ob.ask_lvl_1_qty) / 2) AS avg_level_1_price_by_average_qty
      from @{MKT_DATA_T_ORDERBOOK_GLOBEX_10_LVL_DEEP} ob
      WHERE {% condition run_date %} cyc_dt {% endcondition %}
      AND {% condition exchange %} inst_exch_mrkt_id {% endcondition %}
      AND {% condition symbol %} inst_prod_cd {% endcondition %}
      AND {% condition period %} inst_ctrt_prd_cd {% endcondition %}
      )
      SELECT *
      FROM
      (
      SELECT tob.cyc_dt,
      tob.exch_id,
      tob.inst_sym,
      MAX(tob.inst_prod_cd) AS prod_cd,
      MAX(tob.inst_prod_typ) AS prod_type,
      MAX(id.contract_period) AS contract_period,
      MAX(id.put_call_ind) AS put_call_ind,
      MAX(id.strike_px) AS strike_px,
      MAX(num_days_to_expiry) AS num_days_to_expiry,
      --
      MAX(tv.max_vol) AS tot_volume,
      --
      ROUND(AVG(avg_level_1_px_dec), 2) AS avg_top_px_dec,
      ROUND(AVG(avg_level_1_px_dec) / MAX(id.strike_px), 2) AS prct_op_of_strike,
      ROUND(SUM(tob.avg_level_1_price_by_average_qty) / SUM(tob.avg_level_1_qty), 2) AS top_vwap,
      ROUND(AVG(tob.ask_lvl_1_px_dec - tob.bid_lvl_1_px_dec), 2) AS avg_top_bidask_spread,
      APPROX_QUANTILES((tob.ask_lvl_1_px_dec) - (tob.bid_lvl_1_px_dec),2)[OFFSET(1)] AS med_top_bidask_spread
      FROM top_of_book_stats tob
      LEFT JOIN tot_vol tv
      ON tob.cyc_dt = tv.cyc_dt AND tob.inst_sym = tv.inst_sym
      LEFT JOIN inst_details id
      ON tob.cyc_dt = id.run_date AND tob.inst_sym = id.glbx_alias
      GROUP BY cyc_dt, exch_id, inst_sym
      )
      WHERE contract_period IS NOT NULL
      AND tot_volume >= 1
      AND prct_op_of_strike < 0.03 ;;
  }

  dimension: cycle_date_hidden_dimension{
    type: string
    label: "Cycle Date HIDDEN DIMENSION"
    description: "THIS IS A HIDDEN FILED AND USED BY THE CYCLE DATE FILTER"
    convert_tz: no
    datatype: date
    can_filter: yes
    hidden: yes
    sql: ${TABLE}.cyc_dt
      ;;
  }

  filter: run_date  {
    label: "Cycle Date Filter"
    type: date
    convert_tz: no
    datatype: date
    sql: {% condition %} ${cycle_date_hidden_dimension} {% endcondition %};;
    suggest_dimension: cyc_dt
  }

  # filter: run_date {
  #   label: "Cycle Date"
  #   type: date_time
  # }

  filter: symbol {
    label: "Instrument Symbol"
    type: string
    suggest_dimension: inst_sym
  }

  filter: exchange {
    label: "Exchange"
    type: string
    suggest_dimension: exch_id
  }

  filter: period {
    label: "Contract Period (YYYYMM)"
    type: string
    suggest_dimension: contract_period
  }

  dimension: cyc_dt {
    type: date
    label: "Cycle Date"
    datatype: date
    sql: ${TABLE}.cyc_dt ;;
  }

  dimension: exch_id {
    type: string
    label: "Exchange"
    sql: ${TABLE}.exch_id ;;
  }

  dimension: inst_sym {
    type: string
    label: "Instrument Symbol"
    sql: ${TABLE}.inst_sym ;;
  }

  dimension: prod_cd {
    type: string
    label: "Product Code"
    sql: ${TABLE}.prod_cd ;;
  }

  dimension: prod_type {
    type: string
    label: "Produt Type"
    sql: ${TABLE}.prod_type ;;
  }

  dimension: contract_period {
    type: string
    label: "Contract Period"
    sql: ${TABLE}.contract_period ;;
  }

  dimension: put_call_ind {
    type: string
    label: "Put Call Indicator"
    sql: ${TABLE}.put_call_ind ;;
  }

  dimension: strike_px {
    type: number
    label: "Strike Price"
    sql: ${TABLE}.strike_px ;;
  }

  dimension: num_days_to_expiry {
    type: number
    label: "Days Until Expiry"
    sql: ${TABLE}.num_days_to_expiry ;;
  }

  measure: tot_volume {
    type: sum
    label: "Total Volume"
    sql: ${TABLE}.tot_volume ;;
  }

  dimension: avg_top_px_dec {
    type: number
    label: "Avg Top Price"
    sql: ${TABLE}.avg_top_px_dec ;;
  }

  measure: prct_op_of_strike {
    type: sum
    label: "% Op Of Strike"
    sql: ${TABLE}.prct_op_of_strike ;;
  }

  measure: top_vwap {
    type: sum
    label: "Top VWAP"
    sql: ${TABLE}.top_vwap ;;
  }

  measure: avg_top_bidask_spread {
    type: sum
    label: "Avg Top Bid Ask Spread"
    sql: ${TABLE}.avg_top_bidask_spread ;;
  }

  measure: med_top_bidask_spread {
    type: sum
    label: "Median Top Bid Ask Spread"
    sql: ${TABLE}.med_top_bidask_spread ;;
  }
}