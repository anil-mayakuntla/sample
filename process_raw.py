import psycopg2
import pandas as pd

from flask import Flask

# from sqlalchemy import create_engine
#
# df = pd.read_csv('input/db_files/GDC_NPM_2G_Hourly_1.csv')
# engine = create_engine('postgresql://TestUser:test1234@localhost:5432/mapa')
# header = list(df.columns)
# header_low = []
# for col in header:
#     header_low.append(col.lower().replace(' ','_').replace('(','_').replace(')','').replace('__','_'))
# df.columns = header_low
# df.to_sql('huawei_raw_2g_1', engine)
#
# df = pd.read_csv('input/db_files/GDC_NPM_2G_Hourly_2.csv')
# engine = create_engine('postgresql://TestUser:test1234@localhost:5432/mapa')
# header = list(df.columns)
# header_low = []
# for col in header:
#     header_low.append(col.lower().replace(' ','_').replace('(','_').replace(')','').replace('__','_'))
# df.columns = header_low
# df.to_sql('huawei_raw_2g_2', engine)
#
# df = pd.read_csv('input/db_files/GDC_NPM_2G_Hourly_3.csv')
# engine = create_engine('postgresql://TestUser:test1234@localhost:5432/mapa')
# header = list(df.columns)
# header_low = []
# for col in header:
#     header_low.append(col.lower().replace(' ','_').replace('(','_').replace(')','').replace('__','_'))
# df.columns = header_low
# df.to_sql('huawei_raw_2g_3', engine)
#
# df = pd.read_csv('input/db_files/GDC_NPM_2G_Hourly_4.csv')
# engine = create_engine('postgresql://TestUser:test1234@localhost:5432/mapa')
# header = list(df.columns)
# header_low = []
# for col in header:
#     header_low.append(col.lower().replace(' ','_').replace('(','_').replace(')','').replace('__','_'))
# df.columns = header_low
# df.to_sql('huawei_raw_2g_4', engine)
# print('done')


def createConn():
    conn = psycopg2.connect(
        host="localhost",
        database="mapa",
        user="TestUser",
        password="test1234",
        port=5432)

    return conn


def executeQuery(query):
    try:
        conn = createConn()
        cur = conn.cursor()
        cur.execute(query)
        row = cur.fetchone()
        cur.close()
        return row[0]
    except psycopg2.Error as error:
        print(error)
    finally:
        conn.close()


# def bsc():
#     q1 = "SELECT count(distinct(bsc)) FROM sitedb_2g"
#     res = executeQuery(q1)
#     return res
#
#
# def total_sites():
#     q1 = "SELECT count(distinct(site_name)) FROM sitedb_2g where site_name not in ('LMS_SIT')"
#     q2 = "SELECT count(distinct(site_name)) FROM sitedb_2g_temp1 WHERE SitePreference in ('LMS') and Acceptance_Status not in ('Accepted')"
#     res = executeQuery(q1) + executeQuery(q2)
#     return res
#
#
# def macro_indoor():
#     q1 = "SELECT count(distinct(site_name)) FROM sitedb_2g where MACRO_IBS='MACRO' and Macrosite='Indoor' AND site_name not in ('LMS_SIT')"
#     q2 = "SELECT count(distinct(site_name)) FROM sitedb_2g_temp1 WHERE SitePreference in ('LMS') and Acceptance_Status not in ('Accepted') and MACRO_IBS='MACRO' and Macrosite='Indoor'"
#     res = executeQuery(q1) + executeQuery(q2)
#     return res
#
#
# def macro_outdoor():
#     q1 = "SELECT count(distinct(site_name)) FROM sitedb_2g where MACRO_IBS='MACRO' and Macrosite='Outdoor' AND site_name not in ('LMS_SIT')"
#     q2 = "SELECT count(distinct(site_name)) FROM sitedb_2g_temp1 WHERE SitePreference in ('LMS') and Acceptance_Status not in ('Accepted') and MACRO_IBS='MACRO' and Macrosite='Outdoor'"
#     res = executeQuery(q1) + executeQuery(q2)
#     return res
#
#
# def micro():
#     q1 = "SELECT count(distinct(site_name)) FROM sitedb_2g where MACRO_IBS='MICRO' AND site_name not in ('LMS_SIT')"
#     q2 = "SELECT count(distinct(site_name)) FROM sitedb_2g_temp1 WHERE SitePreference in ('LMS') and Acceptance_Status not in ('Accepted') and MACRO_IBS='MICRO'"
#     res = executeQuery(q1) + executeQuery(q2)
#     return res
#
#
# def ibs():
#     q1 = "SELECT count(distinct(site_name)) FROM sitedb_2g where MACRO_IBS='IBS' AND site_name not in ('LMS_SIT')"
#     q2 = "SELECT count(distinct(site_name)) FROM sitedb_2g_temp1 WHERE SitePreference in ('LMS') and Acceptance_Status not in ('Accepted') and MACRO_IBS='IBS'"
#     res = executeQuery(q1) + executeQuery(q2)
#     return eval(str(res))
#
#
# def total_cells():
#     q1 = "SELECT count(distinct(NO)) FROM sitedb_2g"
#     res = executeQuery(q1)
#     return eval(str(res))
#
#
# def total_trxs():
#     q1 = "SELECT sum(Net_TRXs_Available) FROM sitedb_2g"
#     q2 = "SELECT case when sum(Net_TRXs_Available) is null then 0 else sum(Net_TRXs_Available) end FROM sitedb_2g_temp1 WHERE SitePreference in ('LMS') and Acceptance_Status not in ('Accepted')"
#     res = executeQuery(q1) + executeQuery(q2)
#     return eval(str(res))
#
#
# def edge_enabled_trxs():
#     q1 = "SELECT sum(Net_TRXs_Available) FROM sitedb_2g"
#     q2 = "SELECT  case when sum(Net_TRXs_Available) is null then 0 else sum(Net_TRXs_Available) end FROM sitedb_2g_temp1 WHERE SitePreference in ('LMS') and Acceptance_Status not in ('Accepted')"
#     res = executeQuery(q1) + executeQuery(q2)
#     return eval(str(res))
#
#
# def total_no_of__dedicated_data_ts():
#     q1 = "SELECT sum(FPDCH) FROM sitedb_2g"
#     q2 = "SELECT  case when sum(FPDCH) is null then 0 else sum(FPDCH) end FROM sitedb_2g_temp1 WHERE SitePreference in ('LMS') and Acceptance_Status not in ('Accepted')"
#     res = executeQuery(q1) + executeQuery(q2)
#     return eval(str(res))
#
#
# def equipped_radio_network_capacity_macro():
#     q1 = "SELECT sum(Equipped_Erlang_Capacity_forVoice) FROM sitedb_2g where MACRO_IBS='MACRO'"
#     q2 = "SELECT case when sum(Equipped_Erlang_Capacity_forVoice) is null then 0 else sum(Equipped_Erlang_Capacity_forVoice) end FROM sitedb_2g_temp1 WHERE SitePreference in ('LMS') and Acceptance_Status not in ('Accepted')"
#     res = executeQuery(q1) + executeQuery(q2)
#     return eval(str(res))
#
#
# def equipped_radio_network_capacity_micro():
#     q1 = "SELECT case when sum(Equipped_Erlang_Capacity_forVoice) is null then 0 else sum(Equipped_Erlang_Capacity_forVoice) end FROM sitedb_2g where MACRO_IBS='MICRO'"
#     q2 = "select case when sum(Equipped_Erlang_Capacity_forVoice) is null then 0 else sum(Equipped_Erlang_Capacity_forVoice) end FROM sitedb_2g_temp1 where MACRO_IBS='MICRO' and SitePreference in ('LMS') and Acceptance_Status not in ('Accepted')"
#     res = executeQuery(q1) + executeQuery(q2)
#     return eval(str(res))
#
#
# def equipped_radio_network_capacity_ibs():
#     q1 = "SELECT case when sum(Equipped_Erlang_Capacity_forVoice) is null then 0 else sum(Equipped_Erlang_Capacity_forVoice) end FROM sitedb_2g where macro_ibs='IBS'"
#     q2 = "SELECT case when sum(Equipped_Erlang_Capacity_forVoice) is null then 0 else sum(Equipped_Erlang_Capacity_forVoice) end FROM sitedb_2g_temp1 where macro_ibs='IBS' and SitePreference in ('LMS') and Acceptance_Status not in ('Accepted')"
#     res = executeQuery(q1) + executeQuery(q2)
#     return eval(str(res))


def cnrh():
    # cnrh
    attached = executeQuery(
        "select sum(attached) from cnrh_2g where (left(split_part(Period_start_time, ' ', 2),length(split_part(Period_start_time, ' ', 2))-6))= (SELECT left(split_part(Period_start_time, ' ', 2),length(split_part(Period_start_time, ' ', 2))-6) FROM cnrh_2g group by period_start_time order by sum(BH_Traffic_Erl) desc limit 1)")
    detached = executeQuery(
        "select sum(Total_VLR) - sum(attached) from cnrh_2g where (left(split_part(Period_start_time, ' ', 2),length(split_part(Period_start_time, ' ', 2))-6))= (SELECT left(split_part(Period_start_time, ' ', 2),length(split_part(Period_start_time, ' ', 2))-6) FROM cnrh_2g group by period_start_time order by sum(BH_Traffic_Erl) desc limit 1)")
    total_vlr = executeQuery(
        "select sum(Total_VLR) from cnrh_2g where (left(split_part(Period_start_time, ' ', 2),length(split_part(Period_start_time, ' ', 2))-6))= (SELECT left(split_part(Period_start_time, ' ', 2),length(split_part(Period_start_time, ' ', 2))-6) FROM cnrh_2g group by period_start_time order by sum(BH_Traffic_Erl) desc limit 1)")
    no_of_mo_sms = executeQuery("SELECT sum( No_Of_MO_SMS_Sent_24_Hrs) FROM cnrh_2g")
    no_of_mo_sms_switch = executeQuery("SELECT sum( No_Of_MT_SMS_Delivered_24_Hrs) FROM cnrh_2g")
    sms_delivered_during_busy_hour = executeQuery(
        "select sum(SMS_Delivered_during_Busy_Hour) from cnrh_2g where (left(split_part(Period_start_time, ' ', 2),length(split_part(Period_start_time, ' ', 2))-6))= (SELECT left(split_part(Period_start_time, ' ', 2),length(split_part(Period_start_time, ' ', 2))-6) FROM cnrh_2g group by period_start_time order by sum(BH_Traffic_Erl) desc limit 1)")
    network_busy_hour_start_time = executeQuery(
        "SELECT (left(split_part(Period_start_time, ' ', 2),length(split_part(Period_start_time, ' ', 2))-6)) hr FROM cnrh_2g group by period_start_time order by sum(BH_Traffic_Erl) desc limit 1")
    bh_traffic_erl = executeQuery(
        "select trf from (select sum(BH_Traffic_Erl) trf from cnrh_2g group by Period_start_time) f order by trf desc limit 1")

    # value to be populated(constant/variable)
    # totalMSCCapacityErl =
    # msc_utilisation = executeQuery("SELECT sum(BH_Traffic_Erl) FROM cnrh_2g c group by period_start_time order by sum(BH_Traffic_Erl) desc limit 1")
    # msc_utilisation = (msc_utilisation/totalMSCCapacityErl)*100

    peak_processor_load_24hrs = executeQuery(
        "select avg(max_peak) from (select ENSS_name, max(Peak_Processor_Load) AS max_peak from cnrh_2g group by ENSS_name) dd")

    # gdc_lac_contractual_kpis --> db not found
    # paging_success_rate = executeQuery("SELECT 100*(SUM(Contractual_Paging_Success_RateNUM)/SUM(Contractual_Paging_Success_Rate_DENUM)) FROM gdc_lac_contractual_kpis g where Query_Granularity IN ('2G') and (left(split_part(Period_start_time, ' ', 2),length(split_part(Period_start_time, ' ', 2))-6)) = (SELECT left(split_part(Period_start_time, ' ', 2),length(split_part(Period_start_time, ' ', 2))-6) hr FROM cnrh_2g group by period_start_time order by sum(BH_Traffic_Erl) desc limit 1)")

    cnrhDict = {}
    cnrhDict['attached'] = attached
    cnrhDict['detached'] = detached
    cnrhDict['total_vlr'] = total_vlr
    cnrhDict['no_of_mo_sms'] = no_of_mo_sms
    cnrhDict['no_of_mo_sms_switch'] = no_of_mo_sms_switch
    cnrhDict['sms_delivered_during_busy_hour'] = sms_delivered_during_busy_hour
    cnrhDict['network_busy_hour_start_time'] = network_busy_hour_start_time
    cnrhDict['bh_traffic_erl'] = bh_traffic_erl

    return cnrhDict


def sitedb_2g():
    bsc = executeQuery("SELECT count(distinct(bsc)) FROM sitedb_2g")
    total_sites = executeQuery("select (SELECT count(distinct(site_id)) FROM sitedb_2g where site_id not in ('LMS_SIT')) + (SELECT count(distinct(site_id)) FROM sitedb_2g_temp1 WHERE SitePreference in ('LMS') and Acceptance_Status not in ('Accepted'))")
    macro_indoor = executeQuery("select (SELECT count(distinct(site_id)) FROM sitedb_2g where MACRO_IBS='MACRO' and Macrosite='Indoor' AND site_id not in ('LMS_SIT')) + (SELECT count(distinct(site_id)) FROM sitedb_2g_temp1 WHERE SitePreference in ('LMS') and Acceptance_Status not in ('Accepted') and MACRO_IBS='MACRO' and Macrosite='Indoor')")
    macro_outdoor = executeQuery("select (SELECT count(distinct(site_id)) FROM sitedb_2g where MACRO_IBS='MACRO' and Macrosite='Outdoor' AND site_id not in ('LMS_SIT')) + (SELECT count(distinct(site_id)) FROM sitedb_2g_temp1 WHERE SitePreference in ('LMS') and Acceptance_Status not in ('Accepted') and MACRO_IBS='MACRO' and Macrosite='Outdoor')")
    micro = executeQuery("select (SELECT count(distinct(site_id)) FROM sitedb_2g where MACRO_IBS='MICRO' AND site_id not in ('LMS_SIT')) + (SELECT count(distinct(site_id)) FROM sitedb_2g_temp1 WHERE SitePreference in ('LMS') and Acceptance_Status not in ('Accepted') and MACRO_IBS='MICRO')")
    ibs = executeQuery("select (SELECT count(distinct(site_id)) FROM sitedb_2g where MACRO_IBS='IBS' AND site_id not in ('LMS_SIT')) + (SELECT count(distinct(site_id)) FROM sitedb_2g_temp1 WHERE SitePreference in ('LMS') and Acceptance_Status not in ('Accepted') and MACRO_IBS='IBS')")
    total_cells = executeQuery("SELECT count(distinct(oss_cell_id_name)) FROM sitedb_2g")
    total_trxs = eval(str(executeQuery("select (SELECT sum(Net_TRXs_Available) FROM sitedb_2g) + (SELECT case when sum(Net_TRXs_Available) is null then 0 else sum(Net_TRXs_Available) end FROM sitedb_2g_temp1 WHERE SitePreference in ('LMS') and Acceptance_Status not in ('Accepted'))")))
    edge_enabled_trxs = eval(str(executeQuery("select (SELECT sum(Net_TRXs_Available) FROM sitedb_2g) + (SELECT case when sum(Net_TRXs_Available) is null then 0 else sum(Net_TRXs_Available) end FROM sitedb_2g_temp1 WHERE SitePreference in ('LMS') and Acceptance_Status not in ('Accepted'))")))
    total_no_of_dedicated_data_ts = eval(str(executeQuery("select (SELECT sum(FPDCH) FROM sitedb_2g) + (SELECT  case when sum(FPDCH) is null then 0 else sum(FPDCH) end FROM sitedb_2g_temp1 WHERE SitePreference in ('LMS') and Acceptance_Status not in ('Accepted'))")))
    equipped_radio_network_capacity_macro = eval(str(executeQuery("select (SELECT sum(Equipped_Erlang_Capacity_forVoice) FROM sitedb_2g where MACRO_IBS='MACRO') + (SELECT case when sum(Equipped_Erlang_Capacity_forVoice) is null then 0 else sum(Equipped_Erlang_Capacity_forVoice) end FROM sitedb_2g_temp1 WHERE SitePreference in ('LMS') and Acceptance_Status not in ('Accepted'))")))
    equipped_radio_network_capacity_micro = eval(str(executeQuery("select (SELECT case when sum(Equipped_Erlang_Capacity_forVoice) is null then 0 else sum(Equipped_Erlang_Capacity_forVoice) end FROM sitedb_2g where MACRO_IBS='MICRO') + (select case when sum(Equipped_Erlang_Capacity_forVoice) is null then 0 else sum(Equipped_Erlang_Capacity_forVoice) end FROM sitedb_2g_temp1 where MACRO_IBS='MICRO' and SitePreference in ('LMS') and Acceptance_Status not in ('Accepted'))")))
    equipped_radio_network_capacity_ibs = eval(str(executeQuery("select (SELECT case when sum(Equipped_Erlang_Capacity_forVoice) is null then 0 else sum(Equipped_Erlang_Capacity_forVoice) end FROM sitedb_2g where macro_ibs='IBS') + (SELECT case when sum(Equipped_Erlang_Capacity_forVoice) is null then 0 else sum(Equipped_Erlang_Capacity_forVoice) end FROM sitedb_2g_temp1 where macro_ibs='IBS' and SitePreference in ('LMS') and Acceptance_Status not in ('Accepted'))")))
    total_cell_available_time_24hr = eval(str(executeQuery("SELECT count(distinct(oss_cell_id_name))*1440 FROM sitedb_2g")))
    total_cell_available_time_16hr = eval(str(executeQuery("SELECT count(distinct(oss_cell_id_name))*1020 FROM sitedb_2g")))
    no_of_locked_sites = eval(str(executeQuery("SELECT (SELECT count(distinct(site_id)) FROM sitedb_2g_temp where  Acceptance_Status = 'Accepted') - (SELECT count(distinct(site_id)) FROM sitedb_2g_temp where  Acceptance_Status = 'Accepted' and SITESTATUS like'%ACTIVE%')")))
    no_of_locked_cells = eval(str(executeQuery("SELECT count(distinct(oss_cell_id_name)) FROM sitedb_2g_temp where SITESTATUS like'%LOCK%' and Acceptance_Status = 'Accepted'")))
    no_of_locked_trxs = eval(str(executeQuery("SELECT case when sum(Net_TRXs_Available) is null then 0 else sum(Net_TRXs_Available) end  FROM sitedb_2g_temp where SITESTATUS like'%LOCK%'")))
    equipped_radio_network_capacity_locked = eval(str(executeQuery("SELECT case when sum(Equipped_Erlang_Capacity_forVoice) is null then 0 else sum(Equipped_Erlang_Capacity_forVoice) end FROM sitedb_2g_temp where SITESTATUS like'%LOCK%'")))
    total_sites_active_locked = eval(str(executeQuery("select (SELECT count(distinct(site_id)) FROM sitedb_2g_temp where Acceptance_Status = 'Accepted' AND site_id not in ('LMS_SIT')) + (SELECT count(distinct(site_id)) FROM sitedb_2g_temp1 WHERE SitePreference in ('LMS') and Acceptance_Status not in ('Accepted'))")))
    total_cells_active_locked = eval(str(executeQuery("SELECT count(distinct(oss_cell_id_name)) FROM sitedb_2g_temp where Acceptance_Status = 'Accepted'")))
    total_trxs_active_locked = eval(str(executeQuery("SELECT sum(Net_TRXs_Available) FROM sitedb_2g_temp")))
    total_equipped_radio_network_capacity_active_locked = eval(str(executeQuery("SELECT round(cast(sum(Equipped_Erlang_Capacity_forVoice) as numeric), 2) FROM sitedb_2g_temp where lower(SITESTATUS) like '%active%' OR lower(SITESTATUS) like '%lock%'")))

    sitedbDict  = {}
    sitedbDict['bsc'] = bsc
    sitedbDict['total_sites'] = total_sites
    sitedbDict['macro_indoor'] = macro_indoor
    sitedbDict['macro_outdoor'] = macro_outdoor
    sitedbDict['micro'] = micro
    sitedbDict['ibs'] = ibs
    sitedbDict['total_cells'] = total_cells
    sitedbDict['total_trxs'] = total_trxs
    sitedbDict['edge_enabled_trxs'] = edge_enabled_trxs
    sitedbDict['total_no_of_dedicated_data_ts'] = total_no_of_dedicated_data_ts
    sitedbDict['equipped_radio_network_capacity_macro'] = equipped_radio_network_capacity_macro
    sitedbDict['equipped_radio_network_capacity_micro'] = equipped_radio_network_capacity_micro
    sitedbDict['equipped_radio_network_capacity_ibs'] = equipped_radio_network_capacity_ibs
    sitedbDict['total_cell_available_time_24hr'] = total_cell_available_time_24hr
    sitedbDict['total_cell_available_time_16hr'] = total_cell_available_time_16hr
    sitedbDict['no_of_locked_sites'] = no_of_locked_sites
    sitedbDict['no_of_locked_cells'] = no_of_locked_cells
    sitedbDict['no_of_locked_trxs'] = no_of_locked_trxs
    sitedbDict['equipped_radio_network_capacity_locked'] = equipped_radio_network_capacity_locked
    sitedbDict['total_sites_active_locked'] = total_sites_active_locked
    sitedbDict['total_cells_active_locked'] = total_cells_active_locked
    sitedbDict['total_trxs_active_locked'] = total_trxs_active_locked
    sitedbDict['total_equipped_radio_network_capacity_active_locked'] = total_equipped_radio_network_capacity_active_locked

    return sitedbDict


def static_kpi():
    dfStaticKPI = pd.read_csv('./input/kpi_constant.csv')
    dfStaticKPI = dfStaticKPI[['name', 'val']]
    tempDict = {}
    for index, row in dfStaticKPI.iterrows():
        tempDict[row[0].strip().lower()]=row[1]

    static_kpi_dict = {}

    kpis = [
        ' HLR (K) ',
        'No of GMSCs & MSCs',
        ' Total MSC Capacity (Erls) ',
        ' Total MSC Capacity (BHCA) ',
        ' IN Prepaid (Equipped BHCA) ',
        'SMS Capacity (Msg/Sec)',
        ' Call Setup Time ',
        'IREG testing for GSM / GPRS and CAMEL and compliance for Top 25 Operators',
        'Customer Perceived Data Network Service Availability'
        'Round Trip Time',
        'Round Trip Time',
        '% of NW Related Complaints Resolved Within SLA',
        '% of High Value NW Related Complaints Resolved Within SLA',
        '% complaints under Feedback Awaited',
        'CAR',
        'AT pending sites'
    ]

    for i in range(len(kpis)):
        kpis[i] = kpis[i].strip().lower()

    for k,v in tempDict.items():
        if k in kpis:
            static_kpi_dict[k]=eval(v)

    return static_kpi_dict


def network():
    total_bbh_traffic_erlangs = executeQuery("SELECT (sum(Traffic_Full_Rate_Erlangs)+sum(Traffic_Half_Rate_Erlangs)) FROM bbh_2g")
    bbh_traffic_full_rate_erlangs = executeQuery("SELECT sum(Traffic_Full_Rate_Erlangs) FROM bbh_2g")
    bbh_traffic_half_rate_erlangs = executeQuery("SELECT sum(Traffic_Half_Rate_Erlangs) FROM bbh_2g")
    bbh_amr_traffic_full_rate_erlangs = executeQuery("SELECT sum(Traffic_AMR_Full_Rate_Erlangs) FROM bbh_2g")
    bbh_amr_traffic_half_rate_erlangs = executeQuery("SELECT sum(Traffic_AMR_Half_Rate_Erlangs) FROM bbh_2g")
    radio_network_utilization_during_bbh = executeQuery("select((SELECT sum(Total_Traffic_Erlangs) FROM bbh_2g)/((SELECT sum(Equipped_Erlang_Capacity_forVoice) FROM sitedb_2g  where MACRO_IBS='MACRO')+(SELECT case when sum(Equipped_Erlang_Capacity_forVoice) is null then 0  else sum(Equipped_Erlang_Capacity_forVoice) end   FROM sitedb_2g  where MACRO_IBS='MICRO')+(SELECT  case when sum(Equipped_Erlang_Capacity_forVoice) is null then 0 else sum(Equipped_Erlang_Capacity_forVoice) end  FROM sitedb_2g  where MACRO_IBS='IBS'))*100)")
    cells_having_utilization_gt_90 = executeQuery("select((SELECT count((a.Total_Traffic_Erlangs/b.Equipped_Erlang_Capacity_forVoice)*100) kpi FROM bbh_2g a,sitedb_2g b where a.E3GN_name=b.oss_cell_id_name and a.EBSS_name=b.bsc and (a.Total_Traffic_Erlangs/b.Equipped_Erlang_Capacity_forVoice)*100>90)/(SELECT count(distinct(oss_cell_id_name)) FROM sitedb_2g))*100")
    rf_bbh_switch_nbh = executeQuery("select((SELECT sum(Total_Traffic_Erlangs) FROM bbh_2g)/(SELECT sum(BH_Traffic_Erl) FROM cnrh_2g c group by period_start_time order by sum(BH_Traffic_Erl) desc limit 1))")

    total_avg_traffic_erlangs = executeQuery("SELECT ((sum(Traffic_Full_Rate_Erlangs)+sum(Traffic_Half_Rate_Erlangs)))/24 FROM day_2g")
    avg_traffic_full_rate_erlangs = executeQuery("SELECT sum(Traffic_Full_Rate_Erlangs)/24  FROM day_2g")
    avg_traffic_half_rate_erlangs = executeQuery("SELECT sum(Traffic_Half_Rate_Erlangs)/24  FROM day_2g")
    avg_amr_traffic_full_rate_erlangs = executeQuery("SELECT sum(Traffic_AMR_Full_Rate_Erlangs)  FROM day_2g")
    avg_amr_traffic_half_rate_erlangs = executeQuery("SELECT sum(Traffic_AMR_Half_Rate_Erlangs) FROM day_2g")
    sdcch_blocking = executeQuery("select((SELECT sum(SDCCH_Blocking_Nom) FROM day_2g)/(SELECT sum(SDCCH_Blocking_Denom) FROM day_2g))*100")
    sdcch_drop_call_rate = executeQuery("select((SELECT sum(SDCCH_Drop_Nom) FROM day_2g)/(SELECT sum(SDCCH_Drop_Denom) FROM day_2g))*100")
    tch_blocking = executeQuery("select((SELECT sum(TCH_Blocking_Nom) FROM day_2g)/(SELECT sum(TCH_Blocking_Denom) FROM day_2g))*100")
    tch_assignment_success_rate = executeQuery("select  case when ((SELECT sum(TCH_Assig_Success_Rate_Nom) FROM day_2g)/(SELECT sum(TCH_Assig_Success_Rate_Denom) FROM day_2g))*100 >  100 then 100 else ((SELECT sum(TCH_Assig_Success_Rate_Nom) FROM day_2g)/(SELECT sum(TCH_Assig_Success_Rate_Denom) FROM day_2g))*100 end")
    tch_setup_success_rate = executeQuery("select  case when ((SELECT sum(TCH_Assig_Success_Rate_Nom) FROM day_2g)/(SELECT sum(TCH_Assig_Success_Rate_Denom) FROM day_2g))*100 >  100 then 100 else ((SELECT sum(TCH_Assig_Success_Rate_Nom) FROM day_2g)/(SELECT sum(TCH_Assig_Success_Rate_Denom) FROM day_2g))*100 end")
    tch_ho_success_rate = executeQuery("select((SELECT sum(TCH_HO_Success_Rate_Nom) FROM day_2g)/(SELECT sum(TCH_HO_Success_Rate_Denom) FROM day_2g))*100")
    tch_drop_call_rate = executeQuery("select((SELECT sum(TCH_Drop_Nom) FROM day_2g)/(SELECT sum(TCH_Drop_Nom_Denom) FROM day_2g))*100")
    erlangs_min_per_drop = executeQuery("select((SELECT sum(Total_Traffic_Erlangs) FROM day_2g)/(SELECT sum(TCH_Drop_Nom)FROM day_2g ))*60")
    total_vol_data_downloaded_24hrs = executeQuery("SELECT sum(Tot_vol_data_downloaded) FROM day_2g")

    network_avail_total_radio_24hrs = executeQuery("select(((SELECT count(distinct(oss_cell_id_name))*1440 FROM sitedb_2g)+(SELECT count(distinct(oss_cell_id_name))*1440 FROM sitedb_2g_temp1 WHERE SitePreference in ('LMS') and Acceptance_Status not in ('Accepted'))-(SELECT sum(Tot_Cell_Outage_Minutes_) FROM hour_2g a ,sitedb_2g b where  a.cell_name=b.oss_cell_id_name and a.EBSS_name=b.bsc))/((SELECT distinct(count(oss_cell_id_name))*1440 FROM sitedb_2g)+(SELECT count(distinct(oss_cell_id_name))*1440 FROM sitedb_2g_temp1 WHERE SitePreference in ('LMS') and Acceptance_Status not in ('Accepted'))))*100")
    total_cell_outage_mins = executeQuery("SELECT sum(Tot_Cell_Outage_Minutes_) FROM hour_2g a ,sitedb_2g b where  a.cell_name=b.oss_cell_id_name and a.EBSS_name=b.bsc")
    unplanned_cell_outage_mins =executeQuery("SELECT sum(Tot_Cell_Outage_Minutes_) FROM hour_2g a ,sitedb_2g b where  a.cell_name=b.oss_cell_id_name and a.EBSS_name=b.bsc")
    radio_network_avail_excluding_planned = executeQuery("select(((SELECT count(distinct(oss_cell_id_name))*1440 FROM sitedb_2g)+(SELECT count(distinct(oss_cell_id_name))*1440 FROM sitedb_2g_temp1 WHERE SitePreference in ('LMS') and Acceptance_Status not in ('Accepted'))-(SELECT sum(Tot_Cell_Outage_Minutes_) FROM hour_2g a ,sitedb_2g b where  a.cell_name=b.oss_cell_id_name and a.EBSS_name=b.bsc))/((SELECT distinct(count(oss_cell_id_name))*1440 FROM sitedb_2g)+(SELECT count(distinct(oss_cell_id_name))*1440 FROM sitedb_2g_temp1 WHERE SitePreference in ('LMS') and Acceptance_Status not in ('Accepted'))))*100")
    total_cell_outage_mins_07_23 = executeQuery("")
    unplanned_cell_outage_mins_07_23 = executeQuery("")
    radio_network_availability_excluding_planned = executeQuery("select(((SELECT count(distinct(oss_cell_id_name))*1020 FROM sitedb_2g)+(SELECT count(distinct(oss_cell_id_name))*1020 FROM sitedb_2g_temp1 WHERE SitePreference in ('LMS') and Acceptance_Status not in ('Accepted'))-(SELECT sum(Tot_Cell_Outage_Minutes_) FROM hour_2g a ,sitedb_2g b where a.cell_name=b.oss_cell_id_name and a.EBSS_name=b.bsc))/((SELECT distinct(count(oss_cell_id_name))*1020 FROM sitedb_2g)+(SELECT count(distinct(oss_cell_id_name))*1020 FROM sitedb_2g_temp1 WHERE SitePreference in ('LMS') and Acceptance_Status not in ('Accepted'))))*100")
    me_sub_switch_nbh_vlr_attached = executeQuery("select((SELECT sum(BH_Traffic_Erl) FROM cnrh_2g c group by period_start_time order by sum(BH_Traffic_Erl) desc limit 1)/(SELECT sum(Attached) FROM cnrh_2g where (left(split_part(Period_start_time, ' ', 2),length(split_part(Period_start_time, ' ', 2))-6))=(SELECT (left(split_part(Period_start_time, ' ', 2),length(split_part(Period_start_time, ' ', 2))-6)) hr FROM cnrh_2g group by period_start_time order by sum(BH_Traffic_Erl) desc limit 1)))*1000")

    networkDict = {}

    networkDict['total_bbh_traffic_erlangs'] = total_bbh_traffic_erlangs
    networkDict['bbh_traffic_full_rate_erlangs'] = bbh_traffic_full_rate_erlangs
    networkDict['bbh_traffic_half_rate_erlangs'] = bbh_traffic_half_rate_erlangs
    networkDict['bbh_amr_traffic_full_rate_erlangs'] = bbh_amr_traffic_full_rate_erlangs
    networkDict['bbh_amr_traffic_half_rate_erlangs'] = bbh_amr_traffic_half_rate_erlangs
    networkDict['radio_network_utilization_during_bbh'] = radio_network_utilization_during_bbh
    networkDict['cells_having_utilization_gt_90'] = cells_having_utilization_gt_90
    networkDict['rf_bbh_switch_nbh'] = rf_bbh_switch_nbh

    networkDict['total_avg_traffic_erlangs'] = total_avg_traffic_erlangs
    networkDict['avg_traffic_full_rate_erlangs'] = avg_traffic_full_rate_erlangs
    networkDict['avg_traffic_half_rate_erlangs'] = avg_traffic_half_rate_erlangs
    networkDict['avg_amr_traffic_full_rate_erlangs'] = avg_amr_traffic_full_rate_erlangs
    networkDict['avg_amr_traffic_half_rate_erlangs'] = avg_amr_traffic_half_rate_erlangs
    networkDict['sdcch_blocking'] = sdcch_blocking
    networkDict['sdcch_drop_call_rate'] = sdcch_drop_call_rate
    networkDict['tch_blocking'] = tch_blocking
    networkDict['tch_assignment_success_rate'] = tch_assignment_success_rate
    networkDict['tch_setup_success_rate'] = tch_setup_success_rate
    networkDict['tch_ho_success_rate'] = tch_ho_success_rate
    networkDict['tch_drop_call_rate'] = tch_drop_call_rate
    networkDict['erlangs_min_per_drop'] = erlangs_min_per_drop
    networkDict['total_vol_data_downloaded_24hrs'] = total_vol_data_downloaded_24hrs

    return  networkDict


# app = Flask(__name__)
#
#
# @app.route('/generate')
# def gen_report():
#     # df = pd.read_csv('./input/ntwk.csv')
#     # df = df[['Parameter','Calculation Methodology','Report name']]
#     # df = df.dropna()
#     #
#     # consDict={}
#     # df2 = pd.read_csv('./input/kpi_constant.csv')
#     # for index, row in df2.iterrows():
#     #     consDict[str(row[1]).strip().lower()]=row[2]
#     #
#     # kpiDict={}
#     #
#     # for index, row in df.iterrows():
#     #     if str(row[0]).strip() != '-' and str(row[1]).strip() != '-' and str(row[2]).strip() != '-':
#     #         k = row[0].strip()
#     #         if row[1] == 'Static KPI':
#     #             v = eval(consDict[str(row[0]).strip().lower()])
#     #         else:
#     #             v = eval(row[0].strip().replace("'","").replace('.','').replace('(','').replace(')','').lower().replace(' ','_')+'()')
#     #             # temp = str(row[1])
#     #             # if temp.startswith('"'):
#     #             #     temp=temp[1:-1]
#     #             # v = executeQuery(str(temp).strip())
#     #         kpiDict[k] = v
#
#     kpiDict = {}
#     kpiDict.update(static_kpi())
#     kpiDict.update(cnrh())
#     kpiDict.update(sitedb_2g())
#     kpiDict.update(network())
#
#     df3 = pd.read_csv('./input/2g_hourly.csv')
#     date = str(df3[df3.columns[0]][0]).split()[0]
#
#     finalDict = {
#         "Date": date,
#         "Technology": "2G",
#         "Vendor": "Huawei",
#         "kpi_name_value": kpiDict
#     }
#
#     return finalDict
#
#
# @app.route('/')
# def home():
#     return "Report generation"
#
#
# app.run(port=5000)


