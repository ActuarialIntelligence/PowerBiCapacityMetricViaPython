import clr, csv
from azure.storage.blob import BlockBlobService
#use your own DLL path. That is , Extract the 7Z File sent to you and make use of the path to the DLL: Microsoft.AnalysisServices.AdomdClient.dll
clr.AddReference ("..\\Microsoft.AnalysisServices.AdomdClient.dll")
clr.AddReference ("System.Data")
from Microsoft.AnalysisServices.AdomdClient import AdomdConnection , AdomdDataAdapter
from System.Data import DataSet
#use your own server name or address. and data cube name.
#"Data Source=  serverAddress  ;Initial catalog= database ;User ID=
#UserId ;Password= Password;Persist Security Info=True;Impersonation Level=Impersonate"
conn = AdomdConnection("<Connection String>")
conn.Open()
cmd = conn.CreateCommand()
#your MDX query, if you are not familiar, you can use the excel powerpivot to build one query for you.
cmd.CommandText = "EVALUATE TOPN (20,'SystemMetrics')"
adp = AdomdDataAdapter(cmd)
datasetParam =  DataSet()
adp.Fill(datasetParam)
# datasetParam hold your result as collection a\of tables
# each tables has rows
# and each row has columns

with open ('..Path..\\dummy.csv','w', newline='', encoding='utf-8') as file:
    cwriter = csv.writer(file, delimiter=' ',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
#iteration the Dataset and get out a structure 2D data table and save to a file.
    cntr=0
    rowHeader = ''
    for column in range(len(list(datasetParam.Tables[0].Columns))):
        print(str(datasetParam.Tables[0].Columns[column].Caption))
        if cntr  < len(list(datasetParam.Tables[0].Columns)) - 1:
            rowHeader = rowHeader + str(datasetParam.Tables[0].Columns[column].Caption) + ','
            cntr = cntr + 1
        else:
            rowHeader = rowHeader + str(datasetParam.Tables[0].Columns[column].Caption)

    cwriter.writerow(rowHeader)
    for row_n in range(len(list(datasetParam.Tables[0].Rows))):
        row = ''
        cnt=0
        for column_n in range(len(list(datasetParam.Tables[0].Columns))):
            data = datasetParam.Tables[0].Rows[row_n][column_n]
            if cnt  < len(list(datasetParam.Tables[0].Columns))-1:
                row = row + str(data) +  ','
                cnt = cnt + 1
            else:
                row = row + str(data)
        cwriter.writerow(row)
        print(row)

# Upload to Blob Storage
    # A valuable use of this code would be to convert dataa to ByteArray and send this directly to a Serve layer Environment Platform.

        block_blob_service = BlockBlobService(account_name='<INSERT STORAGE ACCOUNT NAME>',
                                              account_key='<INSERT ACCESS KEY>')
        block_blob_service.create_blob_from_path(container_name="<INSERT CONTAINER NAME>",
                                                 blob_name="<INSERT FILE NAME>", file_path="<INSERT FILE PATH>")

conn.Close();


