from pylab import *
import pandas as pd
import matplotlib.pyplot as plt
df = pd.DataFrame( { 'a' : [1, 2, 3, 4], 'b': [ 'w', 'x', 'y', 'z'] })
df
df.info()
df.head(2)
df.tail(2)
df[1:3]
df.describe() #this will give math summary on the numerical columns only.. a in this case

#deal with files:
log_df = pd.read_csv("/home/datascience/wc_day6_1_sample.csv", names=['ClientID', 'Date', 'Time', 'URL', 'ResponseCode', 'Size'], na_values=['-'])

#SQL-like operations
may1_df = log_df[log_df['Date'] == '01/May/1998'] #like the selection operation in relational algebra
url_codes = log_df[['URL', 'ResponseCode']]   #project operation in relational algebra

#grouping
grouped = log_df.groupby('ResponseCode')
grouped.get_group(200)
grouped.describe()  #return math info related to each group for each numberical column
grouped.size()
grouped.sum()
grouped.mean()
grouped.median()
multi_grouped = log_df.groupby(['ResponseCode', 'Date'])
multi_grouped.describe() 

#Apply aggregate functions, pandas use python's lambda functions to define the function
#this creates new columns and apply the lambda fns .. not in place
log_df['DateTime'] = pd.to_datetime(log_df.apply(lambda row: row['Date'] + ' ' + row['Time'], axis=1)) 
log_df['NewResponse'] = pd.to_datetime(log_df.apply(lambda row: row['ResponseCode'] * 3, axis=1)) 

#We can use the new column we added to group data
hour_grouped = log_df.groupby(lambda row: log_df['DateTime'][row].hour)


#Q1 Print the number of requests per client that had HTTP return code 404
from pylab import *
import pandas as pd
import matplotlib.pyplot as plt
log_df = pd.read_csv("/home/datascience/wc_day6_1_sample.csv", names=['ClientID', 'Date', 'Time', 'URL', 'ResponseCode', 'Size'], na_values=['-'])
log1 = log_df[log_df['ResponseCode'] == 404]
log1.groupby('ClientID').size()

#------------------------------------------------------------------------
#Q2 How many requests were made on May 1st before and after noon (12:00:00)
from pylab import *
import pandas as pd
import matplotlib.pyplot as plt
log_df = pd.read_csv("/home/datascience/wc_day6_1_sample.csv", names=['ClientID', 'Date', 'Time', 'URL', 'ResponseCode', 'Size'], na_values=['-'])
log_df['day'] = log_df.apply(lambda row: row['Date'].split('/')[0],axis=1)
log_df['month'] = log_df.apply(lambda row: row['Date'].split('/')[1],axis=1)
log_df['hour'] = log_df.apply(lambda row: row['Time'].split(':')[0],axis=1)

before = log_df[(log_df['month'] == 'May') & (log_df['day'] == '01') & (log_df['hour'] < '12')]
after = log_df[(log_df['month'] == 'May') & (log_df['day'] == '01') & (log_df['hour'] >= '12')]

#------------------------------------------------------------------------
#Q3 What is the average file size for images (.gif, .jpg and .jpeg files), which had response code 200? What is the standard deviation?
#WithCode = log_df[log_df['URL'].str.contains('.')]
from pylab import *
import pandas as pd
import matplotlib.pyplot as plt
log = pd.read_csv("/home/datascience/wc_day6_1_sample.csv", names=['ClientID', 'Date', 'Time', 'URL', 'ResponseCode', 'Size'], na_values=['-'])

log = log[(log['URL'].str.contains(".gif|.jpg|.jpeg")) &(log['ResponseCode'] == 200)]
log['ext'] = log.apply(lambda row: row['URL'].rsplit('.')[1],axis=1)
log = log[log['ext'].str.contains("gif|jpg|jpeg")]
after = log[['ext', 'Size']]
grx = after.groupby('ext')
grx.std()
grx.mean()


#------------------------------------------------------------------------
#Q4 Generate a plot of the number of users of the site every hour. 
from pylab import *
import pandas as pd
import matplotlib.pyplot as plt
log = pd.read_csv("/home/datascience/wc_day6_1_sample.csv", names=['ClientID', 'Date', 'Time', 'URL', 'ResponseCode', 'Size'], na_values=['-'])
log['hour'] = log.apply(lambda row: row['Time'].split(':')[0],axis=1)
log.groupby('hour').size().plot()
plt.xlabel(' hours ')
plt.ylabel(' number of users ')
show()


#------------------------------------------------------------------------
#Q5 If any correlation between clientID/hours of the day. Get 100 clients and generate a scatter plot that shows the hours of the day these clients sent requests. 
#Hint: df.plot(kind='scatter', x='a', y='b'); and df['Column'].unique()

from pylab import *
import pandas as pd
import matplotlib.pyplot as plt
log = pd.read_csv("/home/datascience/wc_day6_1_sample.csv", names=['ClientID', 'Date', 'Time', 'URL', 'ResponseCode', 'Size'], na_values=['-'])
log['hour'] = log.apply(lambda row: row['Time'].split(':')[0],axis=1)
log['day'] = log.apply(lambda row: row['Date'].split('/')[0],axis=1)
df= log[['ClientID','hour','day']]
arr = df['ClientID'].unique()[0:100]
inlist = df[df['ClientID'].isin(arr)]
plt.scatter(inlist.ClientID,inlist.hour,s=20)
plt.xlabel(' Clients ')
plt.ylabel(' hours ')
show()

#------------------------------------------------------------------------
#Q6 Lets apply our analysis to Jul/24 and Jul/25 in the logs. For this start with the raw log file and load it as a Pandas DataFrame. Repeat exercises 3 and 4 with it. How similar or different are the results? Hint: You can use UNIX command line tools from Lab 1 to first get a csv file and then load it into Pandas.

sed 's|/Jul/|/07/|g; s|\([0-9]\+\)/\([0-9]\+\)/\([0-9]\+\):|\3-\2-\1 |; s|"||g; s| +0000]||g; s|- - \[||g; s| |,|g; s@POST,@@g; s@GET,@@g; s@HTTP\/X.X,@@g; s@HTTP\/1.0,@@g; s@HTTP\/1.1,@@g; s|\(.*\),\(.*\),\(.*\),\(.*\),\(.*\),\(.*\),\(.*\)|\1,\2,\3,\5,\6,\7,\4|g' wc_day91_1.log > boom.csv


#for exercies 3:
from pylab import *
import pandas as pd
import matplotlib.pyplot as plt
log = pd.read_csv("/home/datascience/boom.csv", names=['ClientID', 'Date', 'Time', 'URL', 'ResponseCode', 'Size'], na_values=['-'])

log = log[(log['URL'].str.contains(".gif|.jpg|.jpeg")) &(log['ResponseCode'] == 200)]
log['ext'] = log.apply(lambda row: row['URL'].rsplit('.')[1],axis=1)
log = log[log['ext'].str.contains("gif|jpg|jpeg")]
after = log[['ext', 'Size']]
grx = after.groupby('ext')
grx.std()
grx.mean()


#for exercies 4:
from pylab import *
import pandas as pd
import matplotlib.pyplot as plt
log = pd.read_csv("/home/datascience/boom.csv", names=['ClientID', 'Date', 'Time', 'URL', 'ResponseCode', 'Size'], na_values=['-'])
log['hour'] = log.apply(lambda row: row['Time'].split(':')[0],axis=1)
log.groupby('hour').size().plot()
plt.xlabel(' hours ')
plt.ylabel(' number of users ')
show()
