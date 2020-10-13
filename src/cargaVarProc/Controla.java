package cargaVarProc;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import javax.naming.NamingException;
import cargaVarProc.varS;
import circulo_de_credito.Circulo_de_credito;
public class Controla {
    
       
    public void cargaInfoStat(String periodoss) throws SQLException, NamingException, Exception
    {
        
        
        Connection conect=conectar_BD();
        Statement st = conect.createStatement();
            
                ResultSet rs = st.executeQuery("select "
                        + "TRIM(MAIN_CARD),"
                        + "ACCOUNT_NUMBER,"
                        + "MINIMUM_PAYMENT/10000,"
                        + "CREDIT_LIMIT_ML/10000,"
                        + "CONCAT(SIGN_LOCAL_VALUE,BALANCE_LOCAL_VALUE/10000),"
                        + "EXPIRED_BALANCE/10000," +
                          "LAST_PAYABLE_UNTIL \n"+
                        "from dinercap.tbl_statement_typ_1 uno\n" +
                        "inner join dinercap.tbl_statement_typ_2 dos on uno.uuid=dos.uuid\n" +
                        "where payable_until='"+periodoss+"'");
                while (rs.next())
                   {
               try
                {
                       varS varload = new varS();
                       varload.setPeriodo(periodoss);
                       varload.setTarjeta(rs.getObject(1).toString());
                       varload.setCuentaProcesador(rs.getObject(2).toString());
                       System.out.println("ccc "+rs.getObject(2).toString());
                       varload.setMontoPagar(rs.getObject(3).toString());
                       varload.setCreditoMaximo(rs.getObject(4).toString());
                       varload.setSaldoActual(rs.getObject(5).toString());
                       if (Double.parseDouble(varload.getSaldoActual()) < 0 )
                       {
                           varload.setSaldoActual("0");
                       }
                       varload.setSaldoVencido(rs.getObject(6).toString());
                       //Aca va la carga de constantes de la BD
                       varload = cargaConstantes(varload,conect);
                       //Aca continua
                       if (Double.parseDouble(varload.getSaldoVencido()) > 0) //Es moroso
                       {
                          varload = esMoroso(varload,conect);
                          
                        if(varload.getFechaPrimerIncumplimiento() == null)
                        {
                            varload.setFechaPrimerIncumplimiento("0");
                        }
                          
                          
                          if (Long.parseLong(varload.getFechaPrimerIncumplimiento()) <= Long.parseLong("19010101"))
                          {
//                              actualizaData(conect,"update circuloCredito.tbl_circuloConstantes set FechaPrimerIncumplimiento = '"+rs.getObject(7).toString()+"' "
//                                      + "where CuentaActual = '"+varload.getCuentaProcesador()+"'");
                               varload.setFechaIngresoCarteraVencida(rs.getObject(7).toString());
                               varload.setFechaPrimerIncumplimiento(rs.getObject(7).toString());
                              
                          }
                          else
                          {
                              varload.setFechaIngresoCarteraVencida(varload.getFechaPrimerIncumplimiento());
                          }
                          
                            
                       }
                       else
                       {
                           varload.setFechaIngresoCarteraVencida("");
                       }
                       
                       String fechaTempUltimaCompraAntes = varload.getFechaUltimaCompra();
                        
                       varload.setFechaUltimaCompra(ultimaTx(
                               varload.getTarjeta(),
                               varload.getPeriodo(),
                               conect));
                      
                       try
                       {
                       if (Integer.parseInt(varload.getFechaUltimaCompra()) < Integer.parseInt(fechaTempUltimaCompraAntes))
                       {
                           varload.setFechaUltimaCompra(fechaTempUltimaCompraAntes);
                       }
                       }
                       catch(Exception ed)
                       {
                           varload.setFechaUltimaCompra("");
                       }
                       
                       //pago
                        String fechaTempUltimPagoaAntes = varload.getFechaUltimoPago();
                        
                       String[] fechaUltimPago = ultimopag510(varload.getTarjeta(),
                               varload.getPeriodo(),
                                conect);
                       
                       
                       if("19010101".equals(fechaUltimPago[0]))
                       {
                           fechaUltimPago = ultimopagws(varload.getTarjeta(),
                                   varload.getPeriodo(),
                                   conect);
                           
                           varload.setFechaUltimoPago(fechaUltimPago[0]);
                                                      
                           if("19010101".equals(fechaUltimPago[0]))
                           {
                               fechaUltimPago[0] = ultimopagPeriodoAnterior(varload.getCuentaProcesador(),
                                                                        conect);
                               varload.setFechaUltimoPago(fechaUltimPago[0]);
                           }
                       }
                       else
                       {
                           varload.setFechaUltimoPago(fechaUltimPago[0]);
                       }
                       
                       System.out.println("nuevo "+varload.getFechaUltimoPago());
                       System.out.println("viejo "+fechaTempUltimPagoaAntes);
                       try
                       {
                       if (Integer.parseInt(varload.getFechaUltimoPago()) < Integer.parseInt(fechaTempUltimPagoaAntes))
                       {
                           varload.setFechaUltimoPago(fechaTempUltimPagoaAntes);
                       }
                       }
                       catch(Exception ed)
                       {
                           varload.setFechaUltimoPago("0");
                       }
                       varload.setClavePrevencion("0");
                       varload.setSaldoInsoluto(varload.getSaldoActual());
                       
                       if (fechaUltimPago[1] == null)
                       {
                            varload.setMontoUltimoPago("0");
                       }
                       else
                       {
                         varload.setMontoUltimoPago(fechaUltimPago[1]);
                       }
                       
                       String fecPagultemp = varload.getFechaUltimoPago();
                       String fecagapertemp = varload.getFechaAperturaCuenta();
                       String saldoventemp = varload.getSaldoVencido();
                       String saldoactualtemp = varload.getSaldoActual();
                        int uno;
                        int dos;
                        double tre;
                        double cuatro;
                       try
                       {
                           uno = Integer.parseInt(fecPagultemp);
                           dos = Integer.parseInt(fecagapertemp);
                           
                           tre = Double.parseDouble(saldoventemp);
                           cuatro = Double.parseDouble(saldoactualtemp);
                           
                           if(uno < dos)
                           {
                               varload.setFechaUltimoPago("19010101");
                           }
                           if (tre > cuatro)
                           {
                               varload.setSaldoActual(varload.getSaldoVencido());
                           }
                           if (tre > 0)
                                {
                                     String diff="0";
                                    if ("19010101".equals(varload.getFechaUltimoPago()))
                                    {
                                         diff = difmeses(conect,varload.getFechaUltimaCompra(),periodoss);  
                                    }
                                    else
                                    {
                                        diff = difmeses(conect,varload.getFechaUltimoPago(),periodoss);  
                                    }
                                    
                                    varload.setNumeroPagosVencidos(diff);
                                    varload.setPagoActual(diff);
                                }
                       }
                        catch(Exception ex)
                        {
                            
                        }
                       
                       if ("0.0".equals(varload.getSaldoVencido()))
                       {
                           varload.setPagoActual("v");
                           varload.setNumeroPagosVencidos("0");
                       }
                      
                           
                       
                       //
                       varload.setValorActivoValuacion("");
                       varload.setNumeroPagos("");
                       varload.setHistoricoPagos("");
                       varload.setClavePrevencion("");
                       varload.setTotalPagosReportados("");
                       varload.setClaveAnteriorOtorgante("");
                       varload.setNombreAnteriorOtorgante("");
                       varload.setNumeroCuentaAnterior("");
                       varload.setMontoCorrespondientebigintereses("");
                       varload.setFormaPagoActualbigintereses("");
                       varload.setDiasVencimiento("");
                       varload.setPlazoMeses("");
                       varload.setCorreoElectronicoConsumidor("");
                       varload.setTotalSaldosActuales("");
                       varload.setTotalSaldosVencidos("");
                       varload.setTotalElementosNombreReportados("");
                       varload.setTotalElementosDireccionReportados("");
                       varload.setTotalElementosEmpleoReportados("");
                       varload.setTotalElementosCuentaReportados("");
                       varload.setNombreOtorgante_3("");
                       varload.setDomicilioDevolucion("");
                       varload.setGarantia("");
                       
                       //
                       insertaVars(varload,conect);
                       System.out.println("Ok Tarjeta --> "+ varload.getTarjeta());
                                   
                }
                catch (SQLException e)
                    {
                        System.out.println("error-xx-->   "+e);
                    }
                    }
               
            conect.close();
                                             
    }     
    
public void insertaVars(varS mivaar,Connection con) throws SQLException, NamingException, Exception
    {
        
        Statement st = con.createStatement();
        try
            {
                String queryInsert= "insert into dinercap.tbl_reporte_circulo "
                        + "values "
                        + "("
                        + "'"+mivaar.getClaveActualOtorgante().trim()+"',"
                        + "'"+mivaar.getNombreOtorgante().trim()+"',"
                        + "'"+mivaar.getIdentificadorDeMedio().trim()+"',"
                        + "'"+Circulo_de_credito.PeriodoGlobal+"',"
                        + "'"+mivaar.getNotaOtorgante().trim()+"',"
                        + "'"+mivaar.getVersion().trim()+"',"
                        + "'"+mivaar.getApellidoPaterno().trim()+"',"
                        + "'"+mivaar.getApellidoMaterno().trim()+"',"
                        + "'"+mivaar.getApellidoAdicional().trim()+"',"
                        + "'"+mivaar.getNombres().trim()+"',"
                        + "'"+mivaar.getFechaNacimiento().trim()+"',"
                        + "'"+mivaar.getRFC().trim().trim()+"',"
                        + "'"+mivaar.getCURP().trim()+"',"
                        + "'"+mivaar.getNumeroSeguridadSocial().trim()+"',"
                        + "'"+mivaar.getNacionalidad().trim()+"',"
                        + "'"+mivaar.getResidencia().trim()+"',"
                        + "'"+mivaar.getNumeroLicenciaConducir().trim()+"',"
                        + "'"+mivaar.getEstadoCivil().trim()+"',"
                        + "'"+mivaar.getSexo().trim()+"',"
                        + "'"+mivaar.getClaveElectorIFE().trim()+"',"
                        + "'"+mivaar.getNumeroDependientes().trim()+"',"
                        + "'"+mivaar.getFechaDefuncion().trim()+"',"
                        + "'"+mivaar.getIndicadorDefuncion().trim()+"',"
                        + "'"+mivaar.getTipoPersona().trim()+"',"
                        + "'"+mivaar.getDireccion().trim()+"',"
                        + "'"+mivaar.getColoniaPoblacion().trim()+"',"
                        + "'"+mivaar.getDelegacionMunicipio().trim()+"',"
                        + "'"+mivaar.getCiudad().trim()+"',"
                        + "'"+mivaar.getEstado().trim()+"',"
                        + "'"+mivaar.getCP().trim()+"',"
                        + "'"+mivaar.getFechaResidencia().trim()+"',"
                        + "'"+mivaar.getNumeroTelefono().trim()+"',"
                        + "'"+mivaar.getTipoDomicilio().trim()+"',"
                        + "'"+mivaar.getTipoAsentamiento().trim()+"',"
                        + "'"+mivaar.getOrigenDomicilio().trim()+"',"
                        + "'"+mivaar.getNombreEmpresa().trim()+"',"
                        + "'"+mivaar.getDireccion_2().trim()+"',"
                        + "'"+mivaar.getColoniaPoblacion_2().trim()+"',"
                        + "'"+mivaar.getDelegacionMunicipio_2().trim()+"',"
                        + "'"+mivaar.getCiudad_2().trim()+"',"
                        + "'"+mivaar.getEstado_2().trim()+"',"
                        + "'"+mivaar.getCP_2().trim()+"',"
                        + "'"+mivaar.getNumeroTelefono_2().trim()+"',"
                        + "'"+mivaar.getExtension().trim()+"',"
                        + "'"+mivaar.getFax().trim()+"',"
                        + "'"+mivaar.getPuesto().trim()+"',"
                        + "'"+mivaar.getFechaContratacion().trim()+"',"
                        + "'"+mivaar.getClaveMoneda().trim()+"',"
                        + "'"+mivaar.getSalarioMensual().trim()+"',"
                        + "'"+mivaar.getFechaUltimoDiaEmpleo().trim()+"',"
                        + "'"+mivaar.getFechaVerificacionEmpleo().trim()+"',"
                        + "'"+mivaar.getOrigenRazonSocialDomicilio().trim()+"',"
                        + "'"+mivaar.getClaveActualOtorgante().trim()+"',"
                        + "'"+mivaar.getNombreOtorgante().trim()+"',"
                        + "'"+mivaar.getCuentaActual().trim()+"',"
                        + "'"+mivaar.getTipoResponsabilidad().trim()+"',"
                        + "'"+mivaar.getTipoCuenta().trim()+"',"
                        + "'"+mivaar.getTipoContrato().trim()+"',"
                        + "'"+mivaar.getClaveUnidadMonetaria().trim()+"',"
                        + "'"+mivaar.getValorActivoValuacion().trim()+"',"
                        + "'"+mivaar.getNumeroPagos().trim()+"',"
                        + "' ',"
                        + "'"+mivaar.getMontoPagar()+"',"
                        + "'"+mivaar.getFechaAperturaCuenta()+"',"
                        + "'"+mivaar.getFechaUltimoPago()+"',"
                        + "'"+mivaar.getFechaUltimaCompra()+"',"
                        + "'"+mivaar.getFechaCierreCuenta()+"',"
                        + "'"+mivaar.getPeriodo()+"',"
                        + "'"+mivaar.getGarantia()+"',"
                        + "'"+mivaar.getCreditoMaximo()+"',"
                        + "'"+mivaar.getSaldoActual()+"',"
                        + "'"+mivaar.getCreditoMaximo()+"',"
                        + "'"+mivaar.getSaldoVencido()+"',"
                        + "'"+mivaar.getNumeroPagosVencidos()+"',"
                        + "'"+mivaar.getPagoActual()+"',"
                        + "'"+mivaar.getHistoricoPagos()+"',"
                        + "'"+mivaar.getClavePrevencion()+"',"
                        + "'"+mivaar.getTotalPagosReportados()+"',"
                        + "'"+mivaar.getClaveAnteriorOtorgante()+"',"
                        + "'"+mivaar.getNombreAnteriorOtorgante()+"',"
                        + "'"+mivaar.getNumeroCuentaAnterior()+"',"
                        + "'"+mivaar.getFechaPrimerIncumplimiento()+"',"
                        + "'"+mivaar.getSaldoInsoluto()+"',"
                        + "'"+mivaar.getMontoUltimoPago()+"',"
                        + "'"+mivaar.getFechaIngresoCarteraVencida()+"',"
                        + "'',"
                        + "'',"
                        + "'',"
                        + "'',"
                        + "'"+mivaar.getCreditoMaximo()+"',"
                        + "'"+mivaar.getCorreoElectronicoConsumidor()+"',"
                        + "'',"
                        + "'',"
                        + "'',"
                        + "'',"
                        + "'',"
                        + "'',"
                        + "'"+mivaar.getNombreOtorgante_3()+"',"
                        + "'"+mivaar.getDomicilioDevolucion()+"',"
                        + "'|"+mivaar.getTarjeta()+"',"
                        + "'00'"
                        + ")";
               // System.out.println(queryInsert);
                st.executeUpdate(queryInsert);
            }
            catch (Exception e)
                {
                    System.out.println("Error ---> " + e);
                }
                      
             }    
    
        
public varS esMoroso(varS mivar, Connection micon) throws SQLException, NamingException, Exception
    {
       Statement st = micon.createStatement(); 
        try
            {
                ResultSet rs = st.executeQuery("select NumeroPagosVencidos, FechaPrimerIncumplimiento\n" +
                                                "from circuloCredito.tbl_circuloConstantes\n" +
                                                "where CuentaActual = '"+mivar.getCuentaActual()+"'");
                while (rs.next())
                   {
                       int numpag = Integer.parseInt(rs.getObject(1).toString());
                       numpag++;
                       mivar.setNumeroPagosVencidos(Integer.toString(numpag));
                       mivar.setPagoActual(Integer.toString(numpag));
                       mivar.setFechaPrimerIncumplimiento(rs.getObject(2).toString());
                  
                   }
            }
            catch (Exception e)
                {
                        mivar.setNumeroPagosVencidos(Integer.toString(0));
                       mivar.setPagoActual(Integer.toString(0));
                       mivar.setFechaPrimerIncumplimiento("0");
                    System.out.println("error---->   "+e);
                }  
        return mivar;
    }
     
     
public void actualizaData(Connection micon, String query) throws SQLException, NamingException, Exception
    {
       try
            {
            PreparedStatement ps = micon.prepareStatement(query);
            ps.executeUpdate();
            ps.close();
            System.out.println("OK update");
            }
            catch (SQLException se)
            {
                System.out.println("nooo update  "+se);
                
            }
    }
  
public String difmeses(Connection micon, String fec1, String fec2) throws SQLException, NamingException, Exception
    {
         Statement st = micon.createStatement(); 
       String fec_ult_comp = "";
        try
            {
                ResultSet rs = st.executeQuery("SELECT TIMESTAMPDIFF(MONTH,  '"+fec1+"', '"+fec2+"')");
                while (rs.next())
                   {
                       fec_ult_comp = rs.getObject(1).toString();
                   }
            }
            catch (Exception e)
                {
                    System.out.println("error---->   "+e);
                    fec_ult_comp="";
                }  
        return fec_ult_comp;
    }

public varS cargaConstantes(varS varload, Connection micon) throws SQLException, NamingException, Exception
    {
      
       Statement st = micon.createStatement(); 
      
        try
            {
                ResultSet rs = st.executeQuery("select  "
                        + "ClaveOtorgante, "
                        + "NombreOtorgante, "
                        + "IdentificadorDeMedio, "
                        + "FechaExtraccion, "
                        + "NotaOtorgante, "
                        + "Version, "
                        + "ApellidoPaterno, "
                        + "ApellidoMaterno, "
                        + "ApellidoAdicional, "
                        + "Nombres, "
                        + "FechaNacimiento, "
                        + "RFC, "
                        + "CURP, "
                        + "NumeroSeguridadSocial, "
                        + "Nacionalidad, "
                        + "Residencia, "
                        + "NumeroLicenciaConducir, "
                        + "EstadoCivil, "
                        + "Sexo, "
                        + "ClaveElectorIFE, "
                        + "NumeroDependientes, "
                        + "FechaDefuncion, "
                        + "IndicadorDefuncion, "
                        + "TipoPersona, "
                        + "Direccion, "
                        + "ColoniaPoblacion, "
                        + "DelegacionMunicipio, "
                        + "Ciudad, "
                        + "Estado, "
                        + "CP, "
                        + "FechaResidencia, "
                        + "NumeroTelefono, "
                        + "TipoDomicilio, "
                        + "TipoAsentamiento, "
                        + "OrigenDomicilio, "
                        + "NombreEmpresa, "
                        + "Direccion_2, "
                        + "ColoniaPoblacion_2, "
                        + "DelegacionMunicipio_2, "
                        + "Ciudad_2, "
                        + "Estado_2, "
                        + "CP_2, "
                        + "NumeroTelefono_2, "
                        + "Extension, "
                        + "Fax, "
                        + "Puesto, "
                        + "FechaContratacion, "
                        + "ClaveMoneda, "
                        + "SalarioMensual, "
                        + "FechaUltimoDiaEmpleo, "
                        + "FechaVerificacionEmpleo, "
                        + "OrigenRazonSocialDomicilio, "
                        + "ClaveActualOtorgante, "
                        + "NombreOtorgante_2, "
                        + "CuentaActual, "
                        + "TipoResponsabilidad, "
                        + "TipoCuenta, "
                        + "TipoContrato, "
                        + "ClaveUnidadMonetaria, "
                        + "FechaAperturaCuenta, "
                        + "FechaUltimoPago, "
                        + "FechaUltimaCompra, "
                        + "FechaCierreCuenta, "
                        + "NumeroPagosVencidos, "
                        + "PagoActual, "
                        + "FechaPrimerIncumplimiento, "
                        + "MontoUltimoPago, "
                        + "FechaIngresoCarteraVencida "
                        + "from circuloCredito.tbl_circuloConstantes\n" +
                                "where CuentaActual = '"+varload.getCuentaProcesador()+"'");
                while (rs.next())
                   {
                       varload.setClaveOtorgante(rs.getObject(1).toString());
                       varload.setNombreOtorgante(rs.getObject(2).toString());
                       varload.setIdentificadorDeMedio(rs.getObject(3).toString());
                       varload.setFechaExtraccion(rs.getObject(4).toString());
                       varload.setNotaOtorgante(rs.getObject(5).toString());
                       varload.setVersion(rs.getObject(6).toString());
                       varload.setApellidoPaterno(rs.getObject(7).toString());
                       varload.setApellidoMaterno(rs.getObject(8).toString());
                       varload.setApellidoAdicional(rs.getObject(9).toString());
                       varload.setNombres(rs.getObject(10).toString());
                       varload.setFechaNacimiento(rs.getObject(11).toString());
                       varload.setRFC(rs.getObject(12).toString());
                       varload.setCURP(rs.getObject(13).toString());
                       varload.setNumeroSeguridadSocial(rs.getObject(14).toString());
                       varload.setNacionalidad(rs.getObject(15).toString());
                       varload.setResidencia(rs.getObject(16).toString());
                       varload.setNumeroLicenciaConducir(rs.getObject(17).toString());
                       varload.setEstadoCivil(rs.getObject(18).toString());
                       varload.setSexo(rs.getObject(19).toString());
                       varload.setClaveElectorIFE(rs.getObject(20).toString());
                       varload.setNumeroDependientes(rs.getObject(21).toString());
                       varload.setFechaDefuncion(rs.getObject(22).toString());
                       varload.setIndicadorDefuncion(rs.getObject(23).toString());
                       varload.setTipoPersona(rs.getObject(24).toString());
                       varload.setDireccion(rs.getObject(25).toString());
                       varload.setColoniaPoblacion(rs.getObject(26).toString());
                       varload.setDelegacionMunicipio(rs.getObject(27).toString());
                       varload.setCiudad(rs.getObject(28).toString());
                       varload.setEstado(rs.getObject(29).toString());
                       varload.setCP(rs.getObject(30).toString());
                       varload.setFechaResidencia(rs.getObject(31).toString());
                       varload.setNumeroTelefono(rs.getObject(32).toString());
                       varload.setTipoDomicilio(rs.getObject(33).toString());
                       varload.setTipoAsentamiento(rs.getObject(34).toString());
                       varload.setOrigenDomicilio(rs.getObject(35).toString());
                       varload.setNombreEmpresa(rs.getObject(36).toString());
                       varload.setDireccion_2(rs.getObject(37).toString());
                       varload.setColoniaPoblacion_2(rs.getObject(38).toString());
                       varload.setDelegacionMunicipio_2(rs.getObject(39).toString());
                       varload.setCiudad_2(rs.getObject(40).toString());
                       varload.setEstado_2(rs.getObject(41).toString());
                       varload.setCP_2(rs.getObject(42).toString());
                       varload.setNumeroTelefono_2(rs.getObject(43).toString());
                       varload.setExtension(rs.getObject(44).toString());
                       varload.setFax(rs.getObject(45).toString());
                       varload.setPuesto(rs.getObject(46).toString());
                       varload.setFechaContratacion(rs.getObject(47).toString());
                       varload.setClaveMoneda(rs.getObject(48).toString());
                       varload.setSalarioMensual(rs.getObject(49).toString());
                       varload.setFechaUltimoDiaEmpleo(rs.getObject(50).toString());
                       varload.setFechaVerificacionEmpleo(rs.getObject(51).toString());
                       varload.setOrigenRazonSocialDomicilio(rs.getObject(52).toString());
                       varload.setClaveActualOtorgante(rs.getObject(53).toString());
                       varload.setNombreOtorgante(rs.getObject(54).toString());
                       varload.setCuentaActual(rs.getObject(55).toString());
                       varload.setTipoResponsabilidad(rs.getObject(56).toString());
                       varload.setTipoCuenta(rs.getObject(57).toString());
                       varload.setTipoContrato(rs.getObject(58).toString());
                       varload.setClaveUnidadMonetaria(rs.getObject(59).toString());
                       varload.setFechaAperturaCuenta(rs.getObject(60).toString());
                       varload.setFechaUltimoPago(rs.getObject(61).toString());
                       varload.setFechaUltimaCompra(rs.getObject(62).toString());
                       System.out.println("fecha "+rs.getObject(62).toString() +"  cuaneta  "+ varload.getCuentaProcesador());
                       varload.setFechaCierreCuenta(rs.getObject(63).toString());
                       varload.setNumeroPagosVencidos(rs.getObject(64).toString());
                       varload.setPagoActual(rs.getObject(65).toString());
                       varload.setFechaPrimerIncumplimiento(rs.getObject(66).toString());
                       varload.setMontoUltimoPago(rs.getObject(67).toString());
                       varload.setFechaIngresoCarteraVencida(rs.getObject(68).toString());
                    }
            }
            catch (Exception e)
                {
                    System.out.println("error---->   "+e);
                }  
        return varload;
    }
   
public String ultimopagPeriodoAnterior(String cuenta, Connection micon) throws SQLException, NamingException, Exception
    {
     
       Statement st = micon.createStatement(); 
       String fec_ult_comp = "";
        try
            {
                ResultSet rs = st.executeQuery("select FechaUltimoPago "
                        + "from circuloCredito.tbl_circuloConstantes "
                        + "where CuentaActual = '"+cuenta+"'");
                while (rs.next())
                   {
                       fec_ult_comp = rs.getObject(1).toString();
                   }
            }
            catch (Exception e)
                {
                    System.out.println("error---->   "+e);
                    fec_ult_comp="";
                }  
        return fec_ult_comp;
    }
  
public String[] ultimopagws(String tarjeta, String periodo, Connection micon) throws SQLException, NamingException, Exception
    {
       Statement st = micon.createStatement(); 
      String[] fec_ult_comp = new String[2];
       fec_ult_comp[0] = "";
        try
            {
                ResultSet rs = st.executeQuery("select DATE_FORMAT(cast(fecha_local as date),'%Y%m%d'),monto_origen/100\n" +
                                                "from dinercap.tbl_auth_pyw\n" +
                                                "where merchant_id='WS MESSAGE'\n" +
                                                "and cast(fecha_local as date) between 19000101 and "+periodo+"\n" +
                                                "and tarjeta ='"+tarjeta+"'\n" +
                                                "order by fecha_local desc \n" +
                                                "limit 1");
                while (rs.next())
                   {
                      fec_ult_comp[0] = rs.getObject(1).toString();
                       fec_ult_comp[1] = rs.getObject(2).toString();
                   }
            }
            catch (Exception e)
                {
                    System.out.println("error---->   "+e);
                      fec_ult_comp[0]="";
                }  
        return fec_ult_comp;
    }
        
public String[] ultimopag510(String tarjeta, String periodo, Connection micon ) throws SQLException, NamingException, Exception
    {
      
       Statement st = micon.createStatement(); 
       String[] fec_ult_comp = new String[2];
       fec_ult_comp[0] = "";
        try
            {
                ResultSet rs = st.executeQuery("select DATE_FORMAT(cast(fecha as date),'%Y%m%d'),importe_ori/100\n" +
                                            "from dinercap.tbl_archivo_510\n" +
                                            "where tipo_tx='20' and fecha between 19000101 and "+periodo+"\n" +
                                            "and tarjeta ='"+tarjeta+"'\n" +
                                            "order by fecha desc \n" +
                                            "limit 1");
                while (rs.next())
                   {
                       fec_ult_comp[0] = rs.getObject(1).toString();
                       fec_ult_comp[1] = rs.getObject(2).toString();
                   }
            }
            catch (Exception e)
                {
                    System.out.println("error---->   "+e);
                    fec_ult_comp[0]="";
                }  
        return fec_ult_comp;
    }
    
    
public String ultimaTx(String tarjeta, String periodo, Connection micon) throws SQLException, NamingException, Exception
    {
       
       Statement st = micon.createStatement(); 
       String fec_ult_comp = "";
        try
            {
                ResultSet rs = st.executeQuery("select DATE_FORMAT(max(cast(fecha_local as date)),'%Y%m%d')\n" +
                    "from dinercap.tbl_auth_pyw\n" +
                    "where cast(fecha_local as date) between 19000101 and "+periodo+"\n" +
                    "and tarjeta ='"+tarjeta+"'");
                while (rs.next())
                   {
                       fec_ult_comp = rs.getObject(1).toString();
                   }
            }
            catch (Exception e)
                {
                    System.out.println("error---->   "+e);
                    fec_ult_comp="";
                }  
        return fec_ult_comp;
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
