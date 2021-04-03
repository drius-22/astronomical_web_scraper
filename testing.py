
from astroquery.vizier import Vizier



v = Vizier(columns=['Vmag', 'b-y','m1','c1','Beta'], catalog="II/215/catalog")
result =  v.query_object('HD  48915')[0][0]
print (result)