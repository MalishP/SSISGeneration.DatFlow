using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Dts.Pipeline;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;

namespace SSISGeneration.DataFlow
{
       public class DataFlow_Factory
    {
        public string Package_File { get; set; }
        public string Package_Container{get;set;}
        public string Source_Conection_Name { get; set; }
        public string Source_Table_Name { get; set; }
        public string Source_Query { get; set; }
        public string Destination_Conection_Name { get; set; }
        public string Destination_Table_Name { get; set; }

       
        //public bool Create_NotExistsConnection { get; set; }
        public DataFlow_Factory()
        {
            Package_File = string.Empty;
            Package_Container = string.Empty;
            Source_Conection_Name = string.Empty;
            Source_Table_Name = string.Empty;
            Destination_Conection_Name = string.Empty;
            Destination_Table_Name = string.Empty;
            Source_Query = string.Empty;
            
            //Create_NotExistsConnection = false;
        }

        public void Insert_DataFlow()
        {
            if(Package_File==string.Empty)
            {
                throw new System.ArgumentException("Empty Package Name", "Package_File");
            }
            if (Source_Conection_Name == string.Empty)
            {
                throw new System.ArgumentException("Empty Source Conection Name", "Source_Conection_Name");
            }
            if (Destination_Conection_Name == string.Empty)
            {
                throw new System.ArgumentException("Empty Destination Conection Name", "Destination_Conection_Name");
            }
            if (Source_Table_Name == string.Empty && Source_Query==string.Empty ) //Mast fill source table or source Query
            {
                throw new System.ArgumentException("Empty Source Table Name and Source Query", "Source_Table_Name");
            }
            
            Source_Query = Source_Query != string.Empty ? Source_Query : "select * from " + Source_Table_Name;

            Microsoft.SqlServer.Dts.Runtime.Application app = new Microsoft.SqlServer.Dts.Runtime.Application();
            
            Package pkg = app.LoadPackage(Package_File, null);

            
            ConnectionManager S_con = GetConnectionByName(pkg, Source_Conection_Name);
            if (S_con == null )
            {
                throw new System.ArgumentException("Can not find source connection by name in Package", "Source_Conection_Name");
            }
            ConnectionManager D_con = GetConnectionByName(pkg, Destination_Conection_Name);
            if (D_con == null)
            {
                throw new System.ArgumentException("Can not find destination connection by name in Package", "Destination_Conection_Name");
            }

            
            TaskHost DataFlowTaskHost = null;
            if (Package_Container != string.Empty)
            {
                DataFlowTaskHost = getNewDataFlowTaskHost(pkg.Executables, Package_Container);
                if (DataFlowTaskHost == null)
                {
                    throw new System.ArgumentException("Can not find Sequence Container ", "Package_Container");
                }
            }
            else
            {
                Executable DFT = pkg.Executables.Add("STOCK:PipelineTask");
                DataFlowTaskHost = DFT as TaskHost;
            }
            
            //
            string s = string.IsNullOrEmpty(Source_Table_Name) ? Destination_Table_Name.Replace("[","_") : Source_Table_Name.Replace("[", "_");
                s=s.Replace("]", "_");
                s=s.Replace(".", "_");
            DataFlowTaskHost.Name = "DTF Load from " + s;
            MainPipe DataFlowTask = DataFlowTaskHost.InnerObject as MainPipe;

            //    / \ : [ ] . =
            //Variables 0
            string d = Destination_Table_Name.Replace("[", "");
            d = d.Replace("]", "");
            Variable Var_DestTable = DataFlowTaskHost.Variables.Add("Destination_table", false, "User", "");
            Var_DestTable.EvaluateAsExpression = true;
            Var_DestTable.Expression = "\"[loading].["+(Destination_Table_Name==string.Empty?Source_Table_Name:Destination_Table_Name)+"_\"+   (DT_STR, 10, 1251)  @[User::system_source_id]+\"]\"";

            Variable myVar2 = DataFlowTaskHost.Variables.Add("SourceSQL", false, "User", Source_Query );


            IDTSComponentMetaData100 source = DataFlowTask.ComponentMetaDataCollection.New();
            source.Name = "OLE DB Source";
            source.ComponentClassID = "DTSAdapter.OleDbSource";
            CManagedComponentWrapper source_instance = source.Instantiate();

            // Initialize the component
            source_instance.ProvideComponentProperties();

            // Specify the connection manager.
            if (source.RuntimeConnectionCollection.Count > 0)
            {
                source.RuntimeConnectionCollection[0].ConnectionManager = DtsConvert.GetExtendedInterface(S_con);
                source.RuntimeConnectionCollection[0].ConnectionManagerID =S_con.ID;
            }

            // Set the custom properties.
           
            source_instance.SetComponentProperty("AccessMode", 2);
            source_instance.SetComponentProperty("SqlCommand",Source_Query);
            if (Source_Query.IndexOf('?') > -1) 
            {
                string ParamMap = string.Empty ;
                string cd = string.Empty; //replace string.Empty with name of variable
                
                foreach (Variable var_day in pkg.Variables )
                {
                    if(var_day.Name==cd)
                    {
                        ParamMap = @"""Parameter0:Input""," + var_day.ID + ";";
                    }
                }
               if(!string.IsNullOrEmpty(ParamMap))
               {
                   source_instance.SetComponentProperty("ParameterMapping", ParamMap);
               }
                
            }


            // Reinitialize the metadata.
            try
            {
                source_instance.AcquireConnections(null);
                source_instance.ReinitializeMetaData();
                source_instance.ReleaseConnections();
            }
            catch 
            {
                throw new System.Exception("Can not access source Table/Query");
            }

            IDTSComponentMetaData100 destination = DataFlowTask.ComponentMetaDataCollection.New();
            destination.Name = "OLE DB Dest";
            destination.ComponentClassID = "DTSAdapter.OLEDBDestination";
            CManagedComponentWrapper destination_instance = destination.Instantiate();

            // Initialize the component
            destination_instance.ProvideComponentProperties();

            // Specify the connection manager.
            if (destination.RuntimeConnectionCollection.Count > 0)
            {
                destination.RuntimeConnectionCollection[0].ConnectionManager = DtsConvert.GetExtendedInterface(D_con);
                destination.RuntimeConnectionCollection[0].ConnectionManagerID = D_con.ID;
            }

            // Set the custom properties.
            destination_instance.SetComponentProperty("AccessMode", 1);
            destination_instance.SetComponentProperty("OpenRowsetVariable", "User::Destination_table");
            //  "select * from dbo.system_source");

            // Reinitialize the metadata.
            try
            {
                destination_instance.AcquireConnections(null);
                destination_instance.ReinitializeMetaData();
                destination_instance.ReleaseConnections();
            }
            catch 
            {
                throw new System.Exception("Can not access destination Table");
            }



            IDTSPath100 path = DataFlowTask.PathCollection.New();
            path.AttachPathAndPropagateNotifications(source.OutputCollection[0],
              destination.InputCollection[0]);

            // Get the destination's default input and virtual input.
            IDTSInput100 input = destination.InputCollection[0];
            IDTSVirtualInput100 vInput = input.GetVirtualInput();

            // Iterate through the virtual input column collection.
            foreach (IDTSVirtualInputColumn100 vColumn in vInput.VirtualInputColumnCollection)
            {
                // Call the SetUsageType method of the destination
                //  to add each available virtual input column as an input column.
                IDTSInputColumn100 vCol = destination_instance.SetUsageType(input.ID, vInput, vColumn.LineageID, DTSUsageType.UT_READONLY);
                destination_instance.MapInputColumn(input.ID, vCol.ID, input.ExternalMetadataColumnCollection[vColumn.Name].ID);
            }
            app.SaveToXml(Package_File, pkg, null);
            
        }

        protected ConnectionManager GetConnectionByName (Package pkg,string ConnectionName)
        {
            ConnectionManager pkg_conn=null;
           
            
            foreach (ConnectionManager con in pkg.Connections)
            {
                if(con.Name==ConnectionName)
                {
                    pkg_conn = con;
                    break;
                }
            }

            return pkg_conn;
        }

        protected TaskHost getNewDataFlowTaskHost(Executables exs,string ContainerName)
        {
            TaskHost DataFlow = null;
            foreach (Executable ex in exs)
            {
                if (ex is Sequence)
                { 
                    Sequence cont=(Sequence)ex;
                    if(cont.Name==ContainerName)
                    {
                        Executable DFT=cont.Executables.Add("STOCK:PipelineTask");
                        DataFlow = DFT as TaskHost;                        
                    }
                    else 
                    {
                        DataFlow=getNewDataFlowTaskHost(cont.Executables , ContainerName);
                    }
                }
                if (ex is ForEachLoop)
                {
                    ForEachLoop cont=(ForEachLoop)ex;
                    if(cont.Name==ContainerName)
                    {
                        Executable DFT=cont.Executables.Add("STOCK:PipelineTask");
                        DataFlow = DFT as TaskHost;
                    }
                    else 
                    {
                        DataFlow=getNewDataFlowTaskHost(cont.Executables , ContainerName);
                    }
                }
                if (ex is ForLoop)
                { 
                     ForLoop cont=(ForLoop)ex;
                    if(cont.Name==ContainerName)
                    {
                        Executable DFT=cont.Executables.Add("STOCK:PipelineTask");
                        DataFlow = DFT as TaskHost;
                    }
                    else 
                    {
                        DataFlow = getNewDataFlowTaskHost(cont.Executables, ContainerName);
                    }
                }
                if (DataFlow!=null)
                {break;}
            }
            return DataFlow;
        }

    }
}
