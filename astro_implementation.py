# SIMBAD and VIZIER Scraper 
# This script makes a connection with Simbad and Vizier databases. It requests the following data
# (HR identifier, HD identifier, coordinates, B magnitue, V magnitude, Spectral type, Mean color index,
#  Mean m1 color index, Mean c1 color index and Mean Beta colo index) and outputs an excel file with the
#  information requested (optional)
#Written by Jorge Lozano
#Last_time_updated: 05/16/2020


import pandas as pd
import io
import re
from astropy.io.votable import parse_single_table
from astropy.table import Table, vstack
import urllib as url_import
#New libraries
import numpy as np
from astroquery.vizier import Vizier
from astroquery.simbad import Simbad 

#Method : Helper
def is_first (flag_in):     
    if flag_in == True: 
        return False
    else:
        return False

#Method: Used in the main class to complement question
def yes_or_no(question):
    answer = input(question + "(y/n): ").lower().strip()
    print("")
    while not(answer == "y" or answer == "yes" or \
    answer == "n" or answer == "no"):
        print("Input yes or no")
        answer = input(question + "(y/n):").lower().strip()
        print("")
    if answer[0] == "y":
        return True
    else:
        return False


#Method: Inputs string of idenfiers and output a list containing each identifier
def convert_to_list (identif):
    
    identif = identif.rsplit(",")
    identif_f = [] #initialization of list

    for cont in identif:
        cont = cont.strip(" ")   
        ##Applies to create range of values
        if cont.count("-") != 0:
            num=[]    
            common_ident = cont[0:2]      
            cont = cont.rsplit("-")  

            for x in cont:            
                num.append(re.sub('\D', '', x)) #looks for numbers within string
                for y in range (int(num[0]),  int(num[1])+1 ):
                    y=str(y)            
                    identif_f.append(common_ident+ y)          
        else: 
            identif_f.append(cont)
    return identif_f


#Method:  (Helper for request_df ()  ) inpust the "IDS" column from dataframe and returns 2 lists: HR identifiers, HD  identifiers 
def split_ids (column_ids):
    
    column_ids = column_ids.values #Data structure swap from pandas.series to narray    
    hr=[]
    hd=[]
    
    for ids_str in column_ids:
        flag_hr = True 
        flag_hd =True       
        ids_str = str(ids_str)
        object_ident =ids_str.rsplit("|") # object_ident becomes a list

        for each_entry in object_ident: 

            each_entry = each_entry.strip(" |'b") 
            HR_pos= each_entry.find("HR")
            HD_pos=each_entry.find("HD")
            
            if (flag_hr ==True):  
                if (HR_pos != -1 ):                # find() method returns -1 if the value is not found
                    hr.append(each_entry)
                    flag_hr = is_first(flag_hr)       
            
            if (flag_hd ==True) :     
                if (HD_pos != -1):
                    hd.append(each_entry)
                    flag_hd = is_first(flag_hd)
               
        if (flag_hr ==True): 
            hr.append(np.NaN)
            flag_hr = is_first(flag_hr)  
        if (flag_hd ==True):     
            hd.append(np.NaN)
            flag_hd = is_first(flag_hd)        
        
    return (hr,hd)

#Method (Helper for request_df() ) inputs a list with HD identifiers and returns a dataframe with same size as simbad_df
def request_vizier (hd_identifiers):  
   
    v = Vizier(columns=['Vmag', 'b-y','m1','c1','Beta'], catalog="II/215/catalog")    
    big_table_hd = Table (names=('Vmag', 'b-y','m1','c1','Beta') )     
     
    for hd_object in hd_identifiers:
        
        hd_object= str(hd_object).strip()         
        
        if not hd_object or hd_object== "NaN" :  #If string is empty  then:  or hd_object== 'NaN'
            big_table_hd.add_row([np.nan, np.nan, np.nan, np.nan,np.nan])            
        else :            
            try:
                result =  v.query_object(hd_object)[0]  
            except IndexError: # This handles if HD identifier does not exist in the catalog
                big_table_hd.add_row([np.nan, np.nan, np.nan, np.nan,np.nan]) 
            else:
                big_table_hd =vstack([big_table_hd, result],join_type='outer', metadata_conflicts='silent') 
            
    df_0 = big_table_hd.to_pandas()
    df_0.drop(df_0.index[0]) #Get rid of random value created at the beginning
    
    return df_0



#Method: Inputs a list of identifers and outputs a datframe with values gathered from Simbad and Vizier 
def request_df (list_identifiers ): 

    #SIMBAD request
    customSimbad =Simbad()
    customSimbad.add_votable_fields('ids','plx','flux(B)','flux(V)','sptype','sp_qual')    
    table = customSimbad.query_objects(list_identifiers) 
    simbad_df = table.to_pandas()                                                        
    simbad_df = simbad_df.filter(['IDS', 'RA','DEC', 'PLX_VALUE', 'FLUX_B', 'FLUX_V', 'SP_TYPE', 'SP_QUAL',])

    #HR and HD numbers come in a string with other identifiers. We find HR and HD identifiers and append 2 new columns to simbad_df
    list_hr, list_hd = split_ids(simbad_df['IDS'])  
    simbad_df.insert (0,"HR",list_hr) # Appends a new column named 'HR' with a list of HR codes 
    simbad_df.insert (1,"HD",list_hd)
    simbad_df = simbad_df.drop('IDS', 1) # deletes the column 'IDS'
    
    simbad_df=simbad_df.astype('str') # converting every entry into string
    simbad_df= simbad_df.applymap(lambda x: x.strip(" 'b")) # gets rid of garbage appended to data
    list_of_HD = simbad_df['HD'].tolist()

    #VIZIER request
    vizier_df = request_vizier(list_of_HD)    
    #Joinning Vizier and Simbad information in one dataframe
    final_df =simbad_df.join(vizier_df)
    
    return  final_df
    



#METHOD: inputs a dataframe and file name (str) and creates an excel file 
def write (datafr, file_name): 
    file_name =file_name.strip(" ") + '.xlsx'
    writer = pd.ExcelWriter(file_name)
    datafr.to_excel(writer,'Sheet1',index=False)
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    format1=workbook.add_format ({'num_format':'#,##0.00000000000000' })
    worksheet.set_column('J:N',18,format1 ) #18 sets column width
    writer.save()

    #file_name =file_name.strip(" ") + '.xlsx'
    #datafr.to_excel(file_name,sheet_name='script_output',index=False, float_format = float_format = ":,")


#MAIN - METHOD 
def main ():

    print ("\n\n*************************** Simbad and Vizier Scraper ***************************")
    main_flag=True

    while(main_flag == True ):  #CHANGE

        final_list_ident = [] #Initializing  list of identifiers        
        in_excel_line = input ("\n\n Enter (excel) to input identifiers from an excel file or (line) to input identifiers from the command line  :")

        in_excel_line = in_excel_line.strip (" ").lower()
        #1)
        if  in_excel_line == "excel" :
            print( "\n \n You selected to input and Excel file. Please follow the guidelines below: \
            \n     +  Identifiers must be arranged in the same excel column 'A' starting from the the very first row \
            \n     +  Name you excel file 'input_file.xlsx'\
            \n     +  'input_file.xlsx' must be in the same directory as the running script\
            \n     +  There must be no empty cell between rows")
            q_excel = "\n\nSelect 'y' to continue  or 'n' to go back.  "
            in_excel = yes_or_no(q_excel) #returns boolean
            if in_excel == True:
                
                excel_df =pd.read_excel ('input_file.xlsx',index_col=None, header=None, usecols= 'A') #Reads all rows of column 'A' from .xlsx file
                # to improve: handle error if file not correctly spelled
                excel_list_ident= excel_df[0].values.tolist()
                final_list_ident = excel_list_ident
                main_flag = is_first(main_flag) #stops while loop
            else:
                continue # current iteration of the loop is disrupted, but the program return to top
        #2)
        elif in_excel_line == "line" :        

            in_line = input ("\n\nEnter all identifieres separated by a comma. For range use '-'.\nE.g. HR7085,HD5896, HR4075-HR4090   :  ")    
            line_list_ident =  convert_to_list (in_line)

            print ( "\n"+ str(line_list_ident) )    
            in_line= yes_or_no (" \n\nPress 'y' if idenfiers are correct or 'n' to input them again :" )
            
            if in_line ==True : # MAKE THIS ONE LINE 
                final_list_ident = line_list_ident
                main_flag = is_first(main_flag)
            else:
                continue
        
        #3)
        else: 
            print ("\n     * Please make sure your spelling is correct.")   
            #in_exit = yes_or_no ("Input 'y' to try again or 'n' to exit the program") IMPROVE BY ASKING IF WANT TO EXIT
            continue

    my_df = request_df (final_list_ident)
    print ("\n\n The first 5 rows of the information requested  : \n\n")
    print (my_df.head(5))
    print (" \n* Values of NaN and nan are substituted for empty strings in the excel sheet")

    my_df = my_df.replace(['NaN', np.nan,'nan'], '', regex=True) #replaces NaN values into empty string
    #Changing column names
    my_df.columns=['HR Identifier','HD Identifier','ICRS Coord-RA','ICRS Coord-DEC','Parallax','B Magn','V Magn','Spectral Type','Type','VizieR V Magn','b-y Index','m1 Index','c1 Index','Beta Index']
    in_export = yes_or_no("\n\nDo you want to export this table to a .xslx file: type 'y' for YES or 'n' to  exit the program ")
        
    if (in_export== True):
        file_name =input("\n Input the name of file without the extension (.xslx) : ") 
        write(my_df,file_name)
        print ("\n Excel file created in the same directory ")
        print ("\n\n*************************** End of Program ***************************")        
    else: 
        print ("\n\n*************************** End of Program ***************************")



if __name__ == "__main__":
    main()