import csv
import logging
import os

from functools import reduce
import pandas 

import planon
import requests
import utils
# *********************************************************************
# LOGGING - set of log messages
# *********************************************************************

log = logging.getLogger(__name__)

# *********************************************************************
# SETUP
# *********************************************************************
planon.PlanonResource.set_site(site=os.environ['PLANON_API_URL'])
planon.PlanonResource.set_header(jwt=os.environ['PLANON_API_KEY'])

#PLANON API
PLANON_API_URL = os.environ['PLANON_API_URL']
log.debug(f"{PLANON_API_URL}")
PLANON_API_KEY = os.environ['PLANON_API_KEY']
log.debug(f"{PLANON_API_KEY}")

#DC API-iPaaS
DARTMOUTH_API_URL = os.environ['DARTMOUTH_API_URL']
log.debug(f"{DARTMOUTH_API_URL}")
DARTMOUTH_API_KEY =  os.environ['DARTMOUTH_API_KEY']
log.debug(f"{DARTMOUTH_API_KEY}")

headers = {
         'Authorization':DARTMOUTH_API_KEY 
      }

scopes= "urn:dartmouth:employees:read.sensitive"

LOGIN_URL =DARTMOUTH_API_URL + "/jwt"
# http://developer.dartmouth.edu/docs/300_resource_apis/250_general_ledger/205_segments.md
EMP_URL=DARTMOUTH_API_URL+"/employees"
CREW_URL=DARTMOUTH_API_URL+"/facilities/crews/"

# ***********************************************************************
# SOURCE DARTMOUTH EMPLOYEES
# Loop through all employees in Dartmouth
# ***********************************************************************
log.debug("Getting Dart employees with iPass from HRMS")
dart_jwt = utils.get_jwt(url=LOGIN_URL,key=DARTMOUTH_API_KEY,scopes=scopes,session=requests.Session())
dart_employees = utils.get_coa(jwt=dart_jwt,url=EMP_URL,session=requests.Session())
# dart_employees = utils.get_coa(jwt=dart_jwt,url=CREW_URL,session=requests.Session())
total_dart_employees = str(len(dart_employees))
Dict_dart_employees ={ dc_emp['netid']: dc_emp for dc_emp in dart_employees} #type:ignore
# log.info(f"Dart_records_Dict :  {str(Dict_dart_employees['dz21041'])}")
log.info(f"Total number of dart_records_Dict : {str(len(Dict_dart_employees))}")

# ***********************************************************************
# SOURCE PLANON PERSONS
# Loop through all Persons in Planon 
# ***********************************************************************
log.debug("Getting Planon Employees")
# pln_employees = planon.Person.find()
# total_pln_Persons = str(len(pln_employees))

pln_filter = {
    "filter": {
        'EmploymenttypeRef' : {'eq': '8'},  #staff=5      
        'FreeString7': {'exists': True},
        'FreeString2': {'eq': 'Active'},
        'IsArchived': {'eq': False},
        'FreeString7': {'eq': 'd1049p2'}
        # 'FreeString7': {'eq': ' d31750h'}
       
  }
}
planon_employees = planon.Person.find(pln_filter)
total_planon_employees = str(len(planon_employees))
Dict_planon_employees={ pln_emp.NetID: pln_emp for pln_emp in planon_employees}
#log.info(f"Dart_records_Dict :  {str(Dict_planon_employees['d26527t'])}")

# ===========================================================================================
# DIFF
# difference between the number of employees between Dart API & Planon
# ===========================================================================================
emp_dart=int(len(dart_employees))
emp_pln=int(len(planon_employees))
diff = emp_dart-emp_pln
log.debug(f"Total number of dart records : {total_dart_employees}")
log.debug(f"Total number of planon records : {total_planon_employees}")
log.debug(f"Difference in numbers of records between Planon and Dartmouth system : {diff}")

# ***********************************************************************
# SOURCE DARTMOUTH CREWS
# Loop through all crews in Dartmouth(HRMS)
# ***********************************************************************
log.debug("Getting Dart crews with iPass from HRMS")
dart_jwt = utils.get_jwt(url=LOGIN_URL,key=DARTMOUTH_API_KEY,scopes=scopes,session=requests.Session())
dart_crews = utils.get_coa(jwt=dart_jwt,url=CREW_URL,session=requests.Session())
Dict_dart_crews ={ dc_crew['id']: dc_crew for dc_crew in dart_crews} #type:ignore
# log.info(f"Dart_records_Dict :  {str(Dict_dart_employees['d31750h'])}")
log.info(f"Total number of dart_records_Dict : {str(len(Dict_dart_crews))}")

# ***********************************************************************
# SOURCE PLANON Labor_Group
# Loop through all Labor_Group in Planon 
# ***********************************************************************
log.debug("Getting planon Labor_Group")
# pln_Labor_Group = planon.Trade.find()
# total_pln_Labor_Group = str(len(pln_Labor_Group))

labor_group_filter = {
    "filter": {
    # "Code":{"exists": True}
    'FreeString1':{"exists": True}
            }
}

planon_labor_group = planon.WorkingHoursTariffGroup.find()
total_planon_labor_group = len(planon_labor_group)
Dict_planon_labor_group ={ coa.Code: coa for coa in planon_labor_group } #type:ignore

# ***********************************************************************
# SOURCE PLANON PERSONTYPES
# Loop through all Persons in Planon 
# ***********************************************************************
log.debug("Getting Planon Person Types")
# pln_employees = planon.Person.find()
# total_pln_Persons = str(len(pln_employees))

pln_filter = {
    "filter": {           
        'Syscode':{'exists': True},
        # 'Syscode':{'eq': '2'},
        'IsActive':{'eq': True},
        # 'Function':{'eq': 'Internal tradesperson'},
        'Freestring7':{'eq': 'd36546b'}
       
               }
}
planon_persontypes = planon.PersonType.find(pln_filter)
total_planon_persontypes = str(len(planon_persontypes))
Dict_planon_persontypes={ pln_emp.Syscode : pln_emp for pln_emp in planon_persontypes}

# ***********************************************************************
# SOURCE PLANON TRADES
# Loop through all Persons in Planon 
# ***********************************************************************
log.debug("Getting Planon Trades")

pln_filter = {
    "filter": {           
        'FreeString11':{'exists':True}
               }
}
planon_trades = planon.Trade.find(pln_filter)
total_planon_trades = str(len(planon_trades))
Dict_planon_trades={ pln_emp.Code : pln_emp for pln_emp in planon_trades}

# ===========================================================================================
# MAIN
# ===========================================================================================
log.info("Starting Labor group feed to Planon :")
# https://www.geeksforgeeks.org/working-csv-files-python/                   
column_headers = ['Trade_Code','Labor_Rate','Person_name']

skip=0
success=0
fail=0
loop=1 
no_dc_crew=0
no_dc_crew_list=0
no_pln_trade=0
no_dc=0
home_dept=0

LaborGroupList  = []
with open(file='load.csv') as file:
    reader = csv.DictReader(file, delimiter=',')

    for row in reader:
        LaborGroupList.append(row)   

# df_csv = pandas.read_csv('load.csv')
# dataframe = df_csv.where(pandas.notnull(df_csv), None)
# crew_dict = dict(zip(dataframe["DC-crew_code"], dataframe["PLN-Syscode"]))

# https://www.geeksforgeeks.org/working-csv-files-python/
column_headers = ['DC_NetID','Planon_NetID','DC_Crew_Code','Planon_Trade_Code']

for pln_employee in planon_employees:
    if pln_employee:
        pln_netid = pln_employee.NetID
        
        try:
            dc_employee = Dict_dart_employees[pln_netid]
            # https://www.w3resource.com/python-exercises/dictionary/python-data-type-dictionary-exercise-52.php            
            # List Comp
            def list_comp(list, key):
                result = [d[key] for d in list if key in d] 
                return result
            
            job_list= dc_employee['jobs']
            
            key='is_home_department'
            homedept_list=list_comp(job_list, key)
            
            key='position_title'
            dc_position_title_list=list_comp(job_list, key)
            dc_position_title=dc_position_title_list[0]

          
            if homedept_list[0]==True:
                key='maintenance_crew' 
                crew_list=list_comp(job_list, key) if homedept_list[0] == True else None
                key ='crew_code'
                crew_code_list=list_comp(crew_list, key)
                key ='crew_shift'
                crew_shift_list=list_comp(crew_list, key)
            
                # changing List Comprehension to String 
                if crew_code_list[0]!=None:
                                
                    try:
                        # dc_employee['crew_code'] = reduce(lambda a, b : a+ " " +str(b), crew_code_list)
                        # dc_employee['crew_shift'] = reduce(lambda a, b : a+ " " +str(b), crew_shift_list)
                        
                        dc_employee['crew_code'] =  crew_code_list[0]

                        if crew_shift_list[0] != None:
                            dc_employee['crew_shift'] =  crew_shift_list[0]
                        else:
                            dc_employee['crew_shift'] =None


                        if dc_employee['crew_code'] != None:
                            if (pln_employee.WorkingHoursTariffGroupRef!=None) and (pln_employee.TradeRef!=None) and (pln_employee.PersonTypeRef!=None) :
                                skip+=1
                                with open("output/skip.csv", 'a') as csvfile:                   
                                    csvwriter = csv.writer(csvfile)
                                    row_data = [dc_employee['netid'], pln_employee.NetID,dc_employee['crew_code'],pln_employee.WorkingHoursTariffGroupRef,pln_employee.PersonTypeRef]                                           
                                    csvwriter.writerow(row_data)
                                    log.info(f"Record {pln_netid} skipped, already has Trade code {pln_employee.WorkingHoursTariffGroupRef}")
                                                    
                                
                            elif(pln_employee.WorkingHoursTariffGroupRef==None) or (pln_employee.TradeRef==None) or (pln_employee.PersonTypeRef==None):
                                log.info(f"Processing netid {pln_netid} with {dc_employee['crew_code']}")

                                pln_netid_filter = {
                                    "filter": {
                                        'FreeString7': {'eq':pln_netid }
                                        }
                                }
                                # UPDATE LABOR_GROUP:                
                                (pln_emp,)= planon.Person.find(pln_netid_filter)

                                #Find all labor groups with labor group code 
                                # pln_labor_group = planon.WorkingHoursTariffGroup.find()
                                # LG_ListComp=[list((i, pln_labor_group[i])) for i in range(len(pln_labor_group))]
                                # code=pln_labor_group[40].Code
                                # syscode=pln_labor_group[40].Syscode

                                # if crew_shift exists then add that to the crew_code
                                if dc_employee['crew_shift'] != None:
                                    dc_employee['crew_code'] = dc_employee['crew_code']+dc_employee['crew_shift']                               
                                    
                                elif dc_employee['crew_shift'] == None:
                                    dc_employee['crew_code']= dc_employee['crew_code']                           
             

                                for row in LaborGroupList:
                                # https://www.geeksforgeeks.org/python-accessing-key-value-in-dictionary/
                                #Assign Planon labor_group to DC labor_group                 
                                # for key,value in crew_dict.items():
                                    try: 
                                        if dc_employee['crew_code'] == row['DC-crew_code']:   
                                            labor_group_syscode = row['PLN-Syscode']
                                            # pln_emp.WorkingHoursTariffGroupRef= 25
                                            # #adding perspontyperef == NONE error handling 
                                            # pln_emp.PersonTypeRef=None
                                            pln_emp.WorkingHoursTariffGroupRef= labor_group_syscode
                                            pln_emp.TradeRef = row['Trade_Syscode']
                                            
                                            # if position-title contains Supervisior then assign them to Internal Coordinator-'12', else Internal Tradesperson-' 2'
                                            target = "Supervisor"
                                            if (dc_position_title)!=None:
                                                if (dc_position_title.__contains__(target)):
                                                    pln_emp.PersonTypeRef= '12'
                                                else:
                                                    pln_emp.PersonTypeRef= ' 2' #add space , so only 2 is represented

                                            pln_emp.save()
                                            # write to CSV file 
                                            success+=1
                                            with open("output/success.csv", 'a') as csvfile:                   
                                                csvwriter = csv.writer(csvfile)
                                                row_data = [dc_employee['netid'], pln_emp.NetID,dc_employee['crew_code'],pln_emp.WorkingHoursTariffGroupRef,dc_employee['home_department'],pln_emp.TradeRef,pln_emp.PersonTypeRef]                                           
                                                csvwriter.writerow(row_data)
                                            log.info(f"Record updated for {pln_netid} with crew_code {dc_employee['crew_code']}")                    
                                            log.info(f"Updated {pln_netid} with {dc_employee['crew_code']}")                                  
                        
                                        elif dc_employee['crew_code'] == None :
                                            no_dc_crew+=1
                                            with open("output/no_dc_crew.csv", 'a') as csvfile:                   
                                                csvwriter = csv.writer(csvfile)
                                                row_data = [dc_employee['netid'], pln_employee.NetID]                                           
                                                csvwriter.writerow(row_data)
                                                log.info(f"Record failed to updated for {pln_netid} - no_dc_crew_code")


                                    except Exception as x:
                                        no_pln_trade+=1
                                        with open("output/no_pln_trade.csv", 'a') as csvfile:                   
                                            csvwriter = csv.writer(csvfile)
                                            row_data = [dc_employee['netid'], pln_emp.NetID]                                           
                                            csvwriter.writerow(row_data)
                                            log.info(f"Record failed to updated for {pln_netid} - no_pln_trade")

                        else :
                            no_dc+=1
                            with open("output/no_dc.csv", 'a') as csvfile:                   
                                csvwriter = csv.writer(csvfile)
                                row_data = [dc_employee['netid'], pln_employee.NetID,pln_employee.PersonTypeRef]                                           
                                csvwriter.writerow(row_data)
                                log.info(f"Record failed to updated for {pln_netid} -no_dc")
                    

                    except Exception as e:                  
                        no_pln_trade+=1
                        with open("output/no_pln_trade.csv", 'a') as csvfile:                   
                            csvwriter = csv.writer(csvfile)
                            row_data = [dc_employee['netid'], pln_employee.NetID,pln_employee.PersonTypeRef]                                           
                            csvwriter.writerow(row_data)
                            log.info(f"Record failed to updated for {pln_netid} -no_pln_trade")

                elif crew_code_list[0]==None:
                    no_dc_crew_list=no_dc_crew_list+1                                  
                    with open("output/no_dc_crew_list.csv", 'a') as csvfile:                   
                        csvwriter = csv.writer(csvfile)                   
                        row_data = [dc_employee['netid'], pln_employee.NetID,pln_employee.PersonTypeRef]                                           
                        csvwriter.writerow(row_data)
                        log.info(f"Record failed to updated for {pln_netid} -no_dc_crew_list")


            elif homedept_list[0]==False:
                home_dept=home_dept+1                                  
                with open("output/home_dept.csv", 'a') as csvfile:                   
                    csvwriter = csv.writer(csvfile)                   
                    row_data = [dc_employee['netid'], pln_employee.NetID,pln_employee.PersonTypeRef]                                           
                    csvwriter.writerow(row_data)

                    
            if loop == len(planon_employees):
                log.info(f"Stopping on loop number : {loop}")
                break       

        except Exception as ex:
            exception = ex
            fail+=1           
            #write to CSV file           
            with open("output/fail.csv", 'a') as csvfile:               
                row_data = [dc_employee['netid'], pln_employee.NetID,pln_employee.PersonTypeRef]
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(row_data)
                log.info(f"Record failed to updated for {pln_netid} with exception{ex}")
                  
            # log.debug(f"Record : {netid} faield with exception {ex}") 
    loop+=1          
    continue              
             


if loop == len(planon_employees):
    log.debug(f"Loop complete ")
else:
    log.debug(f"Something wrong")

