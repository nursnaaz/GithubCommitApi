import datetime
import time
import requests
import json
import math
import pandas as pd
from multiprocessing import Pool
from usersConfig import *


def user_details(args):
    repo_full_name, clientid, clientsecret = args
    totalCommit = 0
    shaId = ''
    owner = ''
    repo = ''
    temp_list = []
    while True:
     for failure_count in range(0, 1):
        #link = 'https://api.github.com/repos/kubernetes/kubernetes/commits?per_page=100'+'?client_id='+clientid+'&client_secret='+clientsecret
        link = 'https://api.github.com/repos/'+ str(repo_full_name) +'/commits?per_page=100&client_id='+ clientid +'&client_secret='+clientsecret+'&sha='+shaId
        print "Link :: "+link

        try:
            r = requests.get(link)  # settimeout
            if (r.ok and r.text != '[]'):

                if '/' in repo_full_name:
                    owner, repo = str(repo_full_name).split('/')
                    print owner, repo
                #print  "total Commit :: "+len(commitItem)
                commitItem = json.loads(r.text or r.content)
                print len(commitItem)
                totalCommit = len(commitItem)
                for i in range(0, len(commitItem)):
                    if ((shaId  != '') and (i==0)):
                        print "exiting first sha"+shaId
                        continue
                    if(commitItem[i]["sha"]=='ew'):
                        break
                    else:
                        try:
                            shaId = commitItem[i]["sha"].encode('utf-8').replace(',', ' ')
                        except:
                            shaId = ''
                        try:
                            authorLogin = commitItem[i]["author"]["login"].encode('utf-8').replace(',', ' ')
                        except:
                            authorLogin = ''
                        try:
                            committerLogin = commitItem[i]["committer"]["login"].encode('utf-8').replace(',', ' ')
                        except:
                            committerLogin = ''
                        try:
                            committerDate = commitItem[i]["commit"]["committer"]["date"].encode('utf-8').replace(',', ' ')
                        except:
                            committerDate = ''
                        try:
                            committed_date = datetime.datetime.now()
                        except:
                            committed_date = ''

                        commitLink = 'https://api.github.com/repos/'+ str(repo_full_name) +'/commits/'+commitItem[i]["sha"]+'?client_id='+ clientid +'&client_secret='+clientsecret
                        print commitLink
                        req = requests.get(commitLink)  # settimeout
                        if (req.ok and req.text != '[]'):
                            commitStats = json.loads(req.text or req.content)
                            try:
                                committsStasTotal = commitStats["stats"]["total"]
                            except:
                                committsStasTotal = -1
                            try:
                                committsStasAdditions = commitStats["stats"]["additions"]
                            except:
                                committsStasAdditions = -1
                            try:
                                committsStasDeletions = commitStats["stats"]["deletions"]
                            except:
                                committsStasDeletions = -1
                        else:
                            print "exception"
                        temp_list.append((str(owner), str(repo), str(shaId), str(authorLogin), str(committerLogin), str(committerDate), str(committsStasTotal),str(committsStasAdditions),str(committsStasDeletions)))
        except Exception, err:
            if failure_count >= 0:
                break
                return ", ".join([str(owner),str(repo), '', '', '', '', '','',''])
            else:
                continue
     if totalCommit == 100:
        print "Greater than 100 commit"
        continue
     else:
         break
    if temp_list:
        q1 = ";".join([str(i) for i in temp_list])
        return q1
    else:
        return ", ".join([str(owner),str(repo), '', '', '','','','',''])


def get_user_details():
    try:
        total_requests = 5000
        requests_per_resource = 2
        calls_per_token = int(total_requests / requests_per_resource)

        #repo_info = pd.read_csv(DATA_LOCATION  + "CiscoUsers/" +"repository_info.csv", encoding='utf-8')
        repo_info = pd.read_csv(DATA_LOCATION  + "testRepo.csv", encoding='utf-8')
        repo_full_names = repo_info['full_name'].values.tolist()
        strList = [str(x) for x in repo_full_names]
        repo_full_name = [x for x in strList if str(x) != 'nan']

        #tokens = pd.read_csv(MISC_LOCATION + "MiscData/" + "githup_api_tokens_40.csv", encoding='utf-8'
        tokens = pd.read_csv(MISC_LOCATION + "githup_api_tokens_40.csv", encoding='utf-8')

        # tokens = pd.read_csv("C:/Cisco/githup_api_tokens_40.csv", encoding='utf-8')
        clientid = tokens[9:10]['Client ID'].values.tolist()
        # clientid = "e43c087b79ffb1d2dfb5"
        clientsecret = tokens[9:10]['Client Secret'].values.tolist()
        # clientsecret = "e61da85ea834b5077d44356bbbd3829c0fb0ab48"
        num_tokens = len(clientid)
        repo_info_df = pd.DataFrame()
        step = 500
        l1, l2 = 0, step
        print "Total number of pages : ", len(repo_full_name)

        start = datetime.datetime.now()
        print "Start time for retrieving Branches Data is: ", start
        range_variable = (int(math.ceil(float(len(repo_full_name)) / step)))

        for i in range(range_variable):
            if l2 > len(repo_full_name):
                l2 = len(repo_full_name)
            print l1, l2
            lis = repo_full_name[l1:l2]
            args = ((li, clientid[0], clientsecret[0]) for li in lis)
            p = Pool(processes=4)
            q0 = p.map(user_details, args)
            # for li in cleanedList:
            #     q0 = getBranchDetails(li,clientid,clientsecret)
            p.close()
            p.join()
            api_output = ";".join(q0)
            api_output = api_output.split(';')
            api_output = [i.replace('(', '').replace(')', '').replace("'", "").split(', ') for i in api_output]
            repo_info_df = repo_info_df.append(api_output)

            print "Size of user_info_df : %s " % len(repo_info_df)
            l1, l2 = l2, l2 + step

            if (l1 % 1000 == 0):
                end = datetime.datetime.now()
                print "End time is: ", end
                time_spent = (end - start).total_seconds()
                if time_spent < 3600:
                    print "Time spent: ", time_spent
                    print "Waiting for the reset of the tokens....\n"
                    time.sleep(3600)
                else:
                    print "Tokens already reset.........\n"
                start = datetime.datetime.now()
                print "Start time for retrieving Commit Data is: ", start


        repo_info_df.columns = ['owner','repo','sha_id', 'author_login', 'committer_login', 'committer_date', 'committs_stas_total', 'committs_stas_additions','committs_stas_deletions']
        # repo_info_df = repo_info_df[repo_info_df['repo_id'] != '']
       # repo_info_df.to_csv(DATA_LOCATION + "CiscoUsers/" + "commit_data.csv", index=False, encoding='utf-8')
        repo_info_df.to_csv(DATA_LOCATION + "commit_data.csv", index=False, encoding='utf-8')

        # repo_info_df.to_csv("C:/Cisco/branches_data.csv", index=False, encoding='utf-8')
        end = datetime.datetime.now()
        print "End time for retrieving Commit Data is : ", end
        print "Total time taken for retrieving Commit Data is : ", (end - start).total_seconds()
    except Exception, e:
        print "Error at getCommit() : %s" % e


if __name__ == '__main__':
    print get_user_details()
    #print user_details()