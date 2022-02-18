import pandas as pd
import os
a = pd.read_csv("job_count.csv")

a.index +=1

# to save as html file
# named as "Table"
#a.to_html("Table.htm")

# assign it to a
# variable (string)
#html_file = a.to_html()


pd.set_option('colheader_justify', 'center')   # FOR TABLE <th>

html_string = '''
<html>
   <body>
         <p>
            Hi Team,
            <br>
            <br>
            We observed that below mentioned users running there jobs in DEFAULT Queue.
            <br>
            <br>
	     Please check the Grafana link for historic data.
             <br> https://github.com/dafauti/voyager/new/main/src/main/python
	<br>
      {table}
      <b><i><p style="color:red">Note: This is an automatically generated email - please do not reply to it.</p>
      If you have any queries please contact us at dafauti13sonu@gmail.com</i></b>
      <br>
      <br>
      Regards,<br>
      Voyager Team
      </p>
   </body>
</html>.
'''

# OUTPUT AN HTML FILE
with open('myhtml.html', 'w') as f:
    f.write(html_string.format(table=a.to_html(classes='mystyle')))
