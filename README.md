# feed_crew_code
This Python automation script will feed the crew_code from Dart API using iPaas and feed it to Planon under WorkManagement >Personnel

## Getting started
```bash:
python -m venv venv
source venv/bin/activate
add source .env to venv/bin/activate (to execute in python shell)
 ```

## Add your files
```
cd existing_repo
git remote add origin https://git.dartmouth.edu/planon/planon-feed/feed_crew_code.git
git branch -M master
git push -uf origin master
```
## Setup:
GL_URL=DARTMOUTH_API_URL+"/general_ledger/entities"

#Dart_dict:
Dict_dart_chart_of_accounts ={ coa['entity']: coa for coa in dart_chart_of_accounts}

#pln_dict:
 pln_filter = {
    "filter": {
    'FreeString11' : {"eq": 'SEG*'}
        
        }
}

dart_flag= dart['parent_child_flag']
dart_code=dart['*******']
dart_desc=dart['*******_description']


#create seg values:
            values = {
                    'Code':row['DC_segment'],
                    'Name':row['DC_segment_description'],
                    'FreeString11':'SEG*'
                    }  

comment this for subactivity , as all subactivities are 'C:
'dart_flag= dart['parent_child_flag']

Only one file can be run at a time , so you have to comment the other files that are not running 