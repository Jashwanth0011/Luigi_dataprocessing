"""
This hiring exercise tests your ability to work with two python packages:  pandas and luigi
It contains two steps which result in a luigi data pipeline that fetchs data from a public
source and saves it to a local .csv file.  Step 1 requires creating a function.  Step 2 requires 
creating a class.  After you create the required function, code at the end of this module will test 
it, and likewise for the required class.

This module contains code that will be used to test your code.  Please refer to the provided code
but do not modify it except where indicated.

Consult online documentation as necessary to complete the steps.

Step 1
------
Create a function named
    createHouseMembersDataFrame()
that fetches information on members of the US House of Representatives using this URL:
https://www.govtrack.us/api/v2/role?current=true&role_type=representative&limit=438

createHouseMembersDataFrame() will accept one argument, which is the URL to fetch from.

createHouseMembersDataFrame() will return a pandas DataFrame object.
For testing, it will be called by the 
provided function createDataFrameAndFile(), included below.

The column names, and column order in the returned DataFrame will be as follows:

['sortname', 'name', 'firstname', 'middlename', 'lastname', 'namemod', 'nickname', 
 'description', 'leadership_title', 'party', 'address', 'phone', 'website']

The above columns represent a portion of the data inclued in the source.
The above column names occur as element names in the source data 
and your DataFrame contents should reflect this matching.
In the source data, some of the required elements are nested within larger data 
structures.  Flatten the data so that each DataFrame column contains data elements 
corresponding to the column name.
Limit the contents of the returned DataFrame to the above columns and re-order the 
columns to conform to the above sequence.

Step 2
------
Create a class that functions as a luigi task.
The code provided below includes class CheckResultOfFetch, which is a luigi task.
Your class should be named and defined so that it functions as the first
task in a luigi pipeline where class CheckResultOfFetch defines the
second task, so that CheckResultOfFetch does its work after your task
completes its work.
The work of your luigi task is to call function createDataFrameAndFile(),
which will call createHouseMembersDataFrame(), which you created in step 1.
createDataFrameAndFile() will create a file containing 
the data returned by createHouseMembersDataFrame(), in .csv form, in the
folder where the python file executing exists.  Provide a file name as
an argument to createHouseMembersDataFrame().

While you test your class, the luigi daemon (luigid.exe) must be 
executing simultaneously.  See luigi online documentation.

"""
import pandas as pd
import luigi
import os
import requests

def createHouseMembersDataFrame(src_url):
    response = requests.get(src_url)

    
    response.raise_for_status()
    data = response.json().get('objects', [])


    records = []
    for item in data:
        record = {
            'sortname': item.get('person', {}).get('sortname', None),
            'name': item.get('person', {}).get('name', None),
            'firstname': item.get('person', {}).get('firstname', None),
            'middlename': item.get('person', {}).get('middlename', None),
            'lastname': item.get('person', {}).get('lastname', None),
            'namemod': item.get('person', {}).get('namemod', None),
            'nickname': item.get('person', {}).get('nickname', None),
            'description': item.get('description', None),
            'leadership_title': item.get('leadership_title', None),
            'party': item.get('party', None),
            'address': item.get('extra', {}).get('address', None),
            'phone': item.get('phone', None),
            'website': item.get('website', None)
        }
        records.append(record)

    # Convert to DataFrame
    df = pd.DataFrame(records)

  
    
    return df[['sortname', 'name', 'firstname', 'middlename', 'lastname', 'namemod', 'nickname',
               'description', 'leadership_title', 'party', 'address', 'phone', 'website']]


class FetchDataFromOrigin(luigi.Task):
    def output(self):
        # Output file path in the current directory
        return luigi.LocalTarget(os.path.join(os.path.dirname(__file__), 'step1.csv'))

    def run(self):
        # Call createDataFrameAndFile() to fetch data and save it to a CSV file
        createDataFrameAndFile(file_name='step1.csv', perform_check=True)

# Add code above this line ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
########################################################################################################################

def createDataFrameAndFile(file_name, perform_check = False):
    """ DO NOT MODIFY THIS FUNCTION
        Creates output data file in the folder where this python code is stored.
        Calls function createHouseMembersDataFrame, which you will create in step 1.
        In step 2, create class FetchDataFromOrigin to execute this function.
    """

    houseMembers = createHouseMembersDataFrame(
        r"https://www.govtrack.us/api/v2/role?current=true&role_type=representative&limit=438")

    if perform_check:
        checkHouseMembersDataFrame(houseMembers)

    if file_name:
        houseMembers.to_csv(os.path.join(os.path.dirname(__file__),
                                         file_name), index=False)

class CheckResultOfFetch(luigi.Task):
    """ DO NOT MODIFY THIS CLASS.
        In step 2, create another class that will allow CheckResultOfFetch to function.
    """
    def requires(self):
        return FetchDataFromOrigin()

    def complete(self):
        return hasattr(self, '_task_complete') and self._task_complete

    def run(self):
        print (f'*\n* In {type(self).__name__}.run()\n*')
        print (f' > Reading file {self.input().path} into a pandas DataFrame...')
        df = pd.read_csv(self.input().path)
        cols = str(list(df.columns))
        row_count = df.shape[0]
        print (f' > Column names: {cols}')
        print (f' > Data row count: {row_count}')
        self._task_complete = True

def checkHouseMembersDataFrame(df):
    """ DO NOT MODIFY THIS FUNCTION """
    assert isinstance(df,pd.DataFrame), "Argument to df is not a pandas DataFrame"
    assert list(df.columns) == ['sortname', 'name', 'firstname', 'middlename', 'lastname', 'namemod', 'nickname', 
                                'description', 'leadership_title', 'party', 'address', 'phone', 'website'], \
        'Column names do not match specification.'
    assert df[df['phone']=='202-225-3265']['lastname'].iloc[0]=='Cohen', \
        'Data row is incorrect for sortname: "Cohen, Steve (Rep.) [D-TN9]"'
    assert df.shape[0] > 430, 'Too few data rows'
    print (f'\n***\n*** House Members DataFrame is correct!\n***')

#######################################################################################################################

""" Test this module 
    You may modify the following, if you choose
"""
if __name__ == '__main__':

    if 'createHouseMembersDataFrame' in dir():
        createDataFrameAndFile(file_name='step1.csv', perform_check = True)

    if 'FetchDataFromOrigin' in dir():
        luigi.run(main_task_cls=CheckResultOfFetch())

#######################################################################################################################
