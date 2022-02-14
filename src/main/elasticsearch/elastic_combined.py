from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch_dsl import *
from elasticsearch_dsl import analysis
from elasticsearch_dsl.connections import connections as es_connections
import csv

es_connections.create_connection(hosts=['https://elastic.elk.lowes.com:9243'],timeout=20)
es=Elasticsearch(['https://elastic.elk.lowes.com:9243'],api_key=('Q0h4VERIc0JmZUZQRVQxNC1hM3g6Ukp2eldoQThTTU9LdWZaNWFKajVSUQ=='))
#               http_auth=("kibana_user","kibanapass")
#               scheme="https")
#            #  port=9200
#           #  )
#           #  )

#es_connections.create_connection(hosts=['https://localhost:9200'],timeout=20)
#es=Elasticsearch()
filename = "/Users/3551341/Documents/combined.csv"
# initializing the titles and rows list
fields = []
rows = []
class GenericAggregationIndex(Document):
    fiscal_year=Integer()
    fiscal_month=Integer()
    term_perc=Double()
    term_days=Double()
    pmt_term_eqvlcy=Double()
    sales=Double()
    unit=Double()
    margin=Double()
    dpp=Double()
    cogs=Double()
    fulfilled_cost=Double()
    vnd_fud_amt=Double()
    tot_clm_aon_amt=Double()
    pos=Double()
    web_pos=Double()
    sta=Double()
    mdf=Double()
    national_promo=Double()
    national_web_promo=Double()
    lump_sum=Double()
    avg_invoice_cost=Double()
    mst=Double()
    rtm_defective_allowance=Double()
    delivery_expense=Double()
    delivery_revenue=Double()
    est_frg_amt=Double()
    tot_dc_rcv_aon_amt=Double()
    tot_dc_shp_aon_amt=Double()
    tot_dc_frg_aon_amt=Double()
    tot_dc_ocp_aon_amt=Double()
    ocn_frg_amt=Double()
    vlm_reb_amt=Double()
    total_transactions=Double()
    dty_amt=Double()
    sos_adj=Double()
    shrink=Double()
    cash_discounts=Double()
    postage=Double()
    non_vat_acmc=Double()
    markup=Double()
    markdown=Double()
    vat=Double()
    merch_margin=Double()
    loadDate=Keyword(normalizer=analysis.normalizer("lowercaseNorm", filter=['lowercase','asciifolding']))
    banner=Keyword(normalizer=analysis.normalizer("lowercaseNorm", filter=['lowercase','asciifolding']))
    #dpp-canada-aggregation-quality-qa
    #dpp-canada-combined-quality-qa
    class Index:
        #name="aggregation-quality"
        name="dpp-canada-combined-quality-prod"
        using=es

def indexdata():
    if es.indices.exists(index='dpp-canada-combined-quality-prod'):
        try:
            es.indices.delete(index='dpp-canada-combined-quality-prod')
        except:
            pass
    GenericAggregationIndex.init()

    # reading csv file
    with open(filename, 'r') as csvfile:
        # creating a csv reader object
        csvreader = csv.reader(csvfile)

        # extracting field names through first row
        fields = next(csvreader)

        # extracting each data row one by one
        for row in csvreader:
            rows.append(row)
    for row in rows:
        try:
            col=row[0].split('|')
            print(col)
            obj=GenericAggregationIndex(fiscal_year=col[0],fiscal_month=col[1],term_perc=col[2] if col[2]!='""' else 0,term_days=col[3] if col[3]!='""' else 0,pmt_term_eqvlcy=col[4] if col[4]!='""' else 0,sales=col[5] if col[5]!='""' else 0,unit=col[6] if col[6]!='""' else 0,margin=col[7] if col[7]!='""' else 0,dpp=col[8] if col[8]!='""' else 0,cogs=col[9] if col[9]!='""' else 0,fulfilled_cost=col[10] if col[10]!='""' else 0,vnd_fud_amt=col[11] if col[11]!='""' else 0,tot_clm_aon_amt=col[12] if col[12]!='""' else 0,pos=col[13] if col[13]!='""' else 0,web_pos=col[14] if col[14]!='""' else 0,sta=col[15] if col[15]!='""' else 0,mdf=col[16] if col[16]!='""' else 0,national_promo=col[17] if col[17]!='""' else 0,national_web_promo=col[18] if col[18]!='""' else 0,lump_sum=col[19] if col[19]!='""' else 0,avg_invoice_cost=col[20] if col[20]!='""' else 0,mst=col[21],rtm_defective_allowance=col[22],delivery_expense=col[23],delivery_revenue=col[24],est_frg_amt=col[25] if col[25]!='""' else 0,	tot_dc_rcv_aon_amt=col[26] if col[26]!='""' else 0,tot_dc_shp_aon_amt=col[27] if col[27]!='""' else 0,tot_dc_frg_aon_amt=col[28] if col[28]!='""' else 0,tot_dc_ocp_aon_amt=col[29] if col[29]!='""' else 0,ocn_frg_amt=col[30] if col[30]!='""' else 0,vlm_reb_amt=col[31] if col[31]!='""' else 0,total_transactions=col[32] if col[32]!='""' else 0,dty_amt=col[33] if col[33]!='""' else 0,sos_adj=col[34] if col[34]!='""' else 0,shrink=col[35] if col[35]!='""' else 0,cash_discounts=col[36] if col[36]!='""' else 0,postage=col[37] if col[37]!='""' else 0,non_vat_acmc=col[38] if col[38]!='""' else 0,markup=col[39] if col[39]!='""' else 0,markdown=col[40] if col[40]!='""' else 0,vat=col[41] if col[41]!='""' else 0,merch_margin=col[42] if col[42]!='""' else 0,loadDate=col[43],banner=col[44])
            obj.save()
        except Exception as e:
            print(str(e))

if __name__=='__main__':
    try:
        indexdata()
    except Exception as e:
        print(str(e))
