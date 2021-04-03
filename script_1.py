import pandas as pd
import io
import re
from astropy.io.votable import parse_single_table
import urllib as url_import





#Method
def is_first (flag_in):
     
    if flag_in == True: 
        return False
    else:
        return False

#Method
def reader (list_identifiers ): 

    flag=True # used to validate 1st dataframe
    df_0=pd.DataFrame()

    for ident in list_identifiers :
    
        values = {'output.format':'votable','Ident':ident, 'output.params':'typed_id,ra,dec,flux(B),flux(V),sp_type,sp_qual'}
        url = "http://simbad.u-strasbg.fr/simbad/sim-id"
        data = url_import.parse.urlencode(values)
        data = data.encode('ascii') # data should be bytes
        req = url_import.request.Request(url, data)

        #Catches possible errors due to bad connection
        try:
            u = url_import.request.urlopen(req) 
        except HTTPError as e:
            print ('The server couldn\'t fulfill the request. ')
            print('Error code: ', e.code)
        except URLError as e:
            print('We failed to reach a server.')
            print('Reason: ', e.reason)
        else:
            #wrap the data in a seekable file-like object
            s=io.BytesIO(u.read()) # This won't work if file is too big, is stored in memory or some temporary processing
            table= parse_single_table(s).to_table()            

            if flag :
                df_0 = table.to_pandas()
                flag= is_first(flag) #Changing value of flag to avoid first table again 
            else:                
                df_next = table.to_pandas()            
                df_0 = df_0.append(df_next,ignore_index=True)     

    df_0.columns=['IDENTIFIER','ICRS COORD-RA','ICRS COORD-DEC','B MAGNITUDE','V MAGNITUDE','SPECTRAL TYPE','TYPE']
    df_0 = df_0.astype('str') #converting every element in str
    df_0= df_0.applymap(lambda x: x.strip(" 'b")) # getting rid of any added character                
    return  df_0
    

#MAIN - METHOD 
def write (datafr, file_name): 
    file_name =file_name.strip(" ") + '.csv'
    datafr.to_csv(file_name, index=False)

def main ():
    input_2='n'

    while(input_2 == 'n'):
        identif = input ("\n\nEnter all identifieres separated by a comma. For range use '-'.\nE.g. HR7085,HD5896, HR4075-HR4090   :  ")    
        identif = identif.rsplit(",")
        identif_f = []

        for cont in identif:
            cont = cont.strip(" ")   
            ###Applies to create range of values
            if cont.count("-") != 0:
                num=[]    
                common_ident = cont[0:2]      
                cont = cont.rsplit("-")  

                for x in cont:            
                    num.append(re.sub('\D', '', x)) #looks for numbers within string

                for y in range (int(num[0]),  int(num[1])+1 ):
                    y=str(y)            
                    identif_f.append(common_ident+ y)
            ####
            else: 
                identif_f.append(cont)
       
        input_2= input("\n" +str(identif_f)+ " \n\nPress 'y' if idenfiers are correct or 'n' to input them again :" )
        input_2 =input_2.strip(" ") .lower()
        
        
    my_df = reader (identif_f)
    print ("\n\n")
    print (my_df.head(5))
    input_3 = input("\n*This is how you table looks like. \n\nDo you want to export it to a .csv file: type 'y' for YES or 'e' to  exit the program ")
    input_3=input_3.strip(" ") .lower()

    if (input_3== 'y'):
        file_name =input("\n Input the name of file without the extension (.csv) : ") 
        write(my_df,file_name)
        print ("\n Excel file created in the same directory ")
        print ("\n\n*************************** End of Program ***************************")
        
    else: 
        print ("\n\n*************************** End of Program ***************************")

if __name__ == "__main__":
    main()