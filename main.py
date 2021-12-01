import sys
import time
import random
import json
sys.path.append('/afs/cern.ch/cms/PPD/PdmV/tools/McM/')
from rest import McM
from stats_rest import Stats2



mcm = McM(dev=False)
stats = Stats2()

mcm_cache = {}
def mcm_get(database, prepid):
    """
    Look for an object with given prepid
    If it is in cache, return cached object
    If not, fetch, save to cache and return it
    """
    if prepid in mcm_cache:
        return mcm_cache[prepid]

    result = mcm.get(database, prepid)
    mcm_cache[prepid] = result
    return result

stats_cache = {}
def stats_get(workflow):
    """
    Look for an object with given prepid
    If it is in cache, return cached object
    If not, fetch, save to cache and return it
    """
    if workflow in stats_cache:
        return stats_cache[workflow]

    try:
        result = stats.get_workflow(workflow)
    except:
        result = {}

    stats_cache[workflow] = result
    return result

def add_workflow(request):
    """
    Look for the latest workflow
    and returns from Stats2 the workflow dictionary
    Otherwise, an empty dictionary
    """
    if not request:
        return
    if request['status'] != 'submitted':
        return
    if request['reqmgr_name']:
        for r in reversed(request['reqmgr_name']):
            if r['content']:
                workflow_name = r['name']
                workflow = stats_get(workflow_name)
                #return workflow
                if workflow:
                    request['workflow'] = {'name': workflow_name,
                                           'priority': workflow['RequestPriority'],
                                           'last_status': workflow['RequestTransition'][-1]['Status'],
                                           'last_status_time': workflow['RequestTransition'][-1]['UpdateTime'],
                                           'type': workflow['RequestType']}
                    return

def make_row(ds, root, mini, nano):
    """
    Make a single output row with given dataset, root, mini and nano requests
    """
    return {'root_prepid': root['prepid'] if root else '',
            'dataset': root['dataset_name'] if root else ds['name'],
            'campaign_count': ds['campaign_count'],
            'pwg_count': ds['pwg_count'],
            'status': root['status'] if root else 'not_exist',
            'root_duplicate_of': root['duplicate_of'] if root else '',
            'root_extension': root['extension'] if root else 0,
            'root_workflow': root.get('workflow') if root else None,
            'mini': mini['prepid'] if mini else '',
            'mini_status': mini['status'] if mini else '',
            'mini_total_events': mini['total_events'] if mini else 0,
            'mini_completed_events': mini['completed_events'] if mini else 0,
            'mini_workflow': mini.get('workflow') if mini else None,
            'nano': nano['prepid'] if nano else '',
            'nano_status': nano['status'] if nano else '',
            'nano_total_events': nano['total_events'] if nano else 0,
            'nano_completed_events': nano['completed_events'] if nano else 0,
            'nano_workflow': nano.get('workflow') if nano else None,
           }

with open('datasets.txt') as ds_file:
    datasets = sorted(list(set([d.strip() for d in ds_file.read().split('\n') if d.strip()])))

print('Read %s datasets from file' % (len(datasets)))
if '--debug' in sys.argv:
    random.shuffle(datasets)
    datasets = datasets[:30]
    print('Picking random %s datasets because debug' % (len(datasets)))

rows = []
for ds_i, ds_name in enumerate(datasets):
    requests = mcm.get('requests', query='prepid=*20UL*GEN*&dataset_name=%s' % (ds_name))
    # filtering out PPD requests
    requests = [r for r in requests if r['pwg'] != 'PPD']
    print('%s/%s dataset %s fetched %s requests' % (ds_i + 1,
                                                    len(datasets),
                                                    ds_name,
                                                    len(requests)))
    if requests:
        duplicates = {}
        # take a set of campaigns for a specif dataset
        # evaluate the length of the set
        # check if it is 4 (20UL should have 4 root campaigns)
        campaign_count = len(set((r['pwg']+r['member_of_campaign']) for r in requests))
        # the same as above but checking if is the same pwg across the root requests
        pwg_count = len(set(r['pwg'] for r in requests))
        for req_i, request in enumerate(requests):
            #print(json.dumps(request, indent=2, sort_keys=True))
            print('  %s/%s request %s' % (req_i + 1, len(requests), request['prepid']))
            add_workflow(request)
            pwg = request['pwg']
            campaign = request['member_of_campaign']
            extension = request['extension']
            # checks if there is a duplicate and returns a prepid
            # at first iteration it would be always return '' (if no '', would return None) because of the get method
            duplicate_of = duplicates.get(pwg,{}).get(campaign,{}).get(extension, '')
            # appending information to the dictionary of the root reuqest
            request['duplicate_of'] = duplicate_of
            # populating the dictionary with pwg if it is not there
            # then populate the pwg with a campaign dictionary
            # then add the extention and prepid to the dictionary of the campaign
            if not duplicate_of:
                duplicates.setdefault(pwg, {}).setdefault(campaign, {})[extension] = request['prepid']
            chain_ids = request['member_of_chain']
            if not chain_ids:
                rows.append(make_row({'name': ds_name, 'campaign_count': campaign_count, 'pwg_count': pwg_count}, request, None, None))
                continue
            for chain_i, chain_id in enumerate(chain_ids):
                print('    %s/%s chained request %s' % (chain_i + 1, len(chain_ids), chain_id))
                # condition to avoid JME Nano chains
                if 'NanoAODJME' in chain_id or 'NanoAODAPVJME' in chain_id: 
                    continue
                # condition to take chains up to nano
                if 'NanoAOD' not in chain_id:
                    continue
                # when you know the exactly thing you wanna fetch, instead of query
                chained_request = mcm_get('chained_requests', chain_id) 
                mini = None
                nano = None
                for req_family in chained_request['chain']:
                    if 'MiniAOD' in req_family:
                        mini = mcm_get('requests', req_family)
                        add_workflow(mini)
                    elif 'NanoAOD' in req_family:
                        nano = mcm_get('requests', req_family)
                        add_workflow(nano)
                rows.append(make_row({'name': ds_name, 'campaign_count': campaign_count, 'pwg_count': pwg_count}, request, mini, nano))
    else:
        # Fake requests to create rows in the table:
        rows.append(make_row({'name': ds_name, 'campaign_count': 0, 'pwg_count': 0}, None, None, None))


with open('data.json', 'w') as output_file:
    json.dump(rows, output_file, indent=2, sort_keys=True)


with open('update_timestamp.txt', 'w') as output_file:
    date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    output_file.write(date)

