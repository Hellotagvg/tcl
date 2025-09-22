import em_reader as em
import calculator as c
import pybybit as pb

def main(identifier,price_a,price_b,symbol,type):
    max_wait = int()
    key_dict = {
    "acc1": {"api_key": "API1", "api_secret": "SECRET1"},
    "acc2": {"api_key": "API2", "api_secret": "SECRET2"}
        } 
    dict_a,dict_b = c.tcl_calc(price_a,price_b,symbol,type)
    pb.trade_tcl(key_dict,dict_a,dict_b,demotrading = True, max_wait_seconds = max_wait )


td_email = 'alerts@'
em.start_email_listener(td_email,callback = main,poll_interval=3,listen_duration=300)
    