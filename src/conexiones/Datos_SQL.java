package conexiones;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.naming.NamingException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import javax.naming.NamingException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.sql.PreparedStatement;
import java.text.DecimalFormat;
import java.util.Properties;
import java.util.regex.Pattern;

public class Datos_SQL 
    {
    
    
public boolean carga_datos_dos() throws SQLException, NamingException, Exception
            {
                boolean resultados=false;
          int y=0;
                 Connection conect=conectar_BD();
                 Statement st = conect.createStatement();
                        try
                          {
//                     ResultSet rs = st.executeQuery("select TRIM(MAIN_CARD),ADDRESS,DISTRICT,CITY,STATE,ZIP\n" +
//"from dinercap.tbl_statement_typ_1 uno\n" +
//"inner join dinercap.tbl_statement_typ_2 dos on uno.uuid=dos.uuid\n" +
//"where payable_until=20191110");
                            
//                          ResultSet rs = st.executeQuery("select TRIM(MAIN_CARD),ACCOUNT_NUMBER,MINIMUM_PAYMENT/10000,CREDIT_LIMIT_ML/10000,CONCAT(SIGN_LOCAL_VALUE,BALANCE_LOCAL_VALUE/10000),EXPIRED_BALANCE/10000\n" +
//"from dinercap.tbl_statement_typ_1 uno\n" +
//"inner join dinercap.tbl_statement_typ_2 dos on uno.uuid=dos.uuid\n" +
//"where payable_until=20191110");
                             
//                           ResultSet rs = st.executeQuery("select tarjeta,max(cast(fecha_local as date))\n" +
//"from dinercap.tbl_auth_pyw\n" +
//"where cast(fecha_local as date) between 20191011 and 20191110\n"
//                                   + "and tarjeta like '506420%'\n" +
//"group by tarjeta");
//                               
//                            ResultSet rs = st.executeQuery("select tarjeta,cast(fecha_local as date)\n" +
//"from dinercap.tbl_auth_pyw \n" +
//"where merchant_id='WS MESSAGE'\n" +
//"and cast(fecha_local as date) between 20191011 and 20191110");
//                              
//                                                            
//                      ResultSet rs = st.executeQuery("select tarjeta,max(fecha)\n" +
//"from dinercap.tbl_archivo_510 \n" +
//"where tipo_tx='20' and fecha between 20191011 and 20191110\n" +
//"group by tarjeta");
//                              ResultSet rs = st.executeQuery("select TRIM(MAIN_CARD),BIRTH_DATE,RG\n" +
//"from dinercap.tbl_statement_typ_1 uno\n" +
//"inner join dinercap.tbl_statement_typ_2 dos on uno.uuid=dos.uuid\n" +
//"where payable_until=20191110");
                                  ResultSet rs = st.executeQuery("select TRIM(MAIN_CARD),min(PAYABLE_UNTIL)\n" +
"from dinercap.tbl_statement_typ_1 uno\n" +
"inner join dinercap.tbl_statement_typ_2 dos on uno.uuid=dos.uuid\n" +
"GROUP BY TRIM(MAIN_CARD) ");
                              
                                         while (rs.next())
                                {
                    
                                  
                                   String tar1=rs.getObject(1).toString(); 
                                   String tar2=rs.getObject(2).toString();
                                 /*   String tar3=rs.getObject(3).toString();
                                 String tar4=rs.getObject(4).toString();
                                   String tar5=rs.getObject(5).toString();
                                   String tar6=rs.getObject(6).toString();
                                    */
                                
                       y++;
                                    System.out.println(y);
                                 
                                
                                    
                   
                               
                           
                                   
                                  carga_datos(tar1,tar2/*,tar3,tar4,tar5,tar6*/);
                                      
                                }
                           
                            }
                
                                            catch (Exception e)
                                                {
                                                    System.out.println("error---->   "+e);
                                                    resultados=false;
                                                }
                  return resultados;                                    
             }     

//uno
public boolean carga_datos_edo() throws SQLException, NamingException, Exception
            {
                boolean resultados=false;
          
                 Connection conect=conectar_BD();
                 Statement st = conect.createStatement();
                        try
                          {
                            ResultSet rs = st.executeQuery("select name,trim(main_card)\n" +
"from dinercap.tbl_statement_typ_1 uno\n" +
"inner join dinercap.tbl_statement_typ_2 dos on uno.uuid=dos.uuid\n" +
"where payable_until=20191110 and CREDIT_LIMIT_ML <>0");
                                         while (rs.next())
                                {
                    
                                   String nom_comple=rs.getObject(1).toString(); 
                                   String tar=rs.getObject(2).toString();
        
                                   String separador = Pattern.quote(" ");
                                   String[] hash=nom_comple.split(separador);
                                   
                                    String ape_1="";
                                    String ape_2="";
                                    String nombrsaa="";
                                 
                                  if (hash.length==8)
                                  {
                                   ape_1=hash[6];
                                   ape_2=hash[7];
                                   nombrsaa=hash[0]+" "+hash[1]+" "+hash[2]+" "+hash[3]+" "+hash[4]+" "+hash[5]/**/;
                                   //  nombrsaa=hash[0];
                                     System.out.println(ape_1);
                                 inserta_tarjeta_nombre(ape_1,ape_2,nombrsaa,tar);
                                   
                                  resultados=true;  
                                  }
                                   
                                   //carga_datos(p_1,p_2);
                                }
                           
                            }
                
                                            catch (Exception e)
                                                {
                                                    resultados=false;
                                                    System.out.println("error---->   "+e);
                                                }
                  return resultados;                                    
             }     
    
    
        //Estado de Cuenta
    public boolean carga_datos(String tarjeta, String tar2 /*,String tar3/*,String tar4,String tar5,String tar6/**/) throws SQLException, NamingException, Exception
            {
          Connection conect=conectar_BD();
           
       try
            {
//           
//                     PreparedStatement ps = conect.prepareStatement(
//             "UPDATE tbl_reporte_circulo SET Direccion = '"+tar2+"',ColoniaPoblacion='"+tar3+"',DelegacionMunicipio='"+tar4+"',Ciudad='"+tar4+"',Estado='"+tar5+"',CP='"+tar6+"'"
//                        + "where trim(tarjeta) = '"+tarjeta.trim()+"'");
               
////             
//                  PreparedStatement ps = conect.prepareStatement(
//                "UPDATE tbl_reporte_circulo SET CuentaActual = '"+tar2+"',MontoPagar='"+tar3+"',CreditoMaximo='"+tar4+"',SaldoActual='"+tar5+"',LimiteCredito='"+tar4+"',SaldoVencido='"+tar6+"',SaldoInsoluto='"+tar5+"',MontoCreditoOriginacion='"+tar4+"'"
//                        + "where trim(tarjeta) = '"+tarjeta.trim()+"'");
////                
//                PreparedStatement ps = conect.prepareStatement(
//                "UPDATE tbl_reporte_circulo SET FechaUltimaCompra = '"+tar2+"'" 
//                        + "where trim(tarjeta) = '"+tarjeta.trim()+"'");
//              
//                    PreparedStatement ps = conect.prepareStatement(
//                "UPDATE tbl_reporte_circulo SET FechaUltimoPago = '"+tar2+"'" 
//                        + "where trim(tarjeta) = '"+tarjeta.trim()+"'");
//                     
//                        PreparedStatement ps = conect.prepareStatement(
//                "UPDATE tbl_reporte_circulo SET FechaNacimiento = '"+tar2+"', RFC='"+tar3+"'" 
//                        + "where trim(tarjeta) = '"+tarjeta.trim()+"'");
//                        
                       PreparedStatement ps = conect.prepareStatement(
                "UPDATE tbl_reporte_circulo SET FechaAperturaCuenta = '"+tar2+"'" 
                        + "where trim(tarjeta) = '"+tarjeta.trim()+"'");
                        
                        
                  ps.executeUpdate();
                      ps.close();
                      System.out.println("OK");
            }
            catch (SQLException se)
            {
              
                System.out.println("nooo"+se);
                
            }
        
        conect.close();
             return true;     
        }          
           
    
    
    
    
      
    public boolean inserta_tarjeta_nombre(String ap_1, String ap_2, String nombres, String tarjeta) throws SQLException, NamingException, Exception
            {
                boolean resultados=false;
                             
                 Connection conect=conectar_BD();
                 Statement st = conect.createStatement();
                                  
                        try
                                        {
                     st.executeUpdate("insert into tbl_reporte_circulo (ApellidoPaterno,ApellidoMaterno,Nombres,tarjeta) values ('"+ap_1+"','"+ap_2+"','"+nombres+"','"+tarjeta+"')");
                                            resultados=true; 
                                          
                                        }
                                            catch (Exception e)
                                                {
                                                    resultados=false;
                                                }
                        conect.close();
                  return resultados;                                    
             }    
    
    
    
    
    
    
public Connection conectar_BD() throws ClassNotFoundException, SQLException, Exception{
        String host_mysql=this.ObtenerHost_mysql();
            String puerto_mysql=this.ObtenerPuerto_mysql();
            String usuario_mysql=this.ObtenerUsuario_mysql();
            String pass_mysql=this.ObtenerPass_mysql();
            String bd_mysql=this.ObtenerBD_mysql();
            Connection conn = null;
            Class.forName("org.gjt.mm.mysql.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://"+host_mysql+":"+
            puerto_mysql+"/"+bd_mysql+"", ""+usuario_mysql+"", ""+pass_mysql+"");
        return conn;
    }  
    public String ObtenerHost_mysql() throws Exception{
        Properties properties = new Properties();
        String valor="";
           try{
                InputStream inputStream = getClass().getResourceAsStream("config.properties");
                properties.load(inputStream);
                valor=properties.getProperty("sql_host");}
            catch (Exception ex){System.out.println("Error ObtenerHost_sql:"+ ex.getLocalizedMessage());throw ex;}
         return valor;
    }
    public String ObtenerPuerto_mysql() throws Exception{
        Properties properties = new Properties();
        String valor="";
           try{
                InputStream inputStream = getClass().getResourceAsStream("config.properties");
                properties.load(inputStream);
                valor=properties.getProperty("sql_puerto");}
            catch (Exception ex){System.out.println("Error ObtenerPuerto_sql:"+ ex.getLocalizedMessage());throw ex;}
         return valor;
    }
    public String ObtenerUsuario_mysql() throws Exception{
           Properties properties = new Properties();
           String valor="";
           try{
                  InputStream inputStream = getClass().getResourceAsStream("config.properties");
                  properties.load(inputStream);
                  valor=properties.getProperty("sql_usuario");}
            catch (Exception ex){System.out.println("Eror al obtener Usuario_sql:"+ ex.getLocalizedMessage());throw ex;}
         return valor;
    }
    public String ObtenerPass_mysql() throws Exception{
           Properties properties = new Properties();
           String valor="";
           try{
                  InputStream inputStream = getClass().getResourceAsStream("config.properties");
                  properties.load(inputStream);
                  valor=properties.getProperty("sql_password");}
            catch (Exception ex){System.out.println("Error al obtener Password_sq:"+ ex.getLocalizedMessage());throw ex;}
         return valor;
    }
    public String ObtenerBD_mysql() throws Exception{
           Properties properties = new Properties();
           String valor="";
           try{
                  InputStream inputStream = getClass().getResourceAsStream("config.properties");
                  properties.load(inputStream);
                  valor=properties.getProperty("sql_base");}
            catch (Exception ex){System.out.println("Error al obtener Base de datos_sq:"+ ex.getLocalizedMessage());throw ex;}
         return valor;
    }
    }

