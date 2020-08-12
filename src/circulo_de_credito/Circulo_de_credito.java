package circulo_de_credito;
import conexiones.Datos_SQL; 
import javax.naming.NamingException;
import cargaVarProc.Controla;
import cargaVarProc.varS;
/**
 *
 * @author leon
 */
public class Circulo_de_credito {

    public static String PeriodoGlobal="";
    
    public static void main(String[] args) throws NamingException, Exception {

      
        PeriodoGlobal = args[0];
        Controla controla = new Controla();
    
        controla.cargaInfoStat(PeriodoGlobal);
    
    }
    
}
