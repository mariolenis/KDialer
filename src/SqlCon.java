/*
 * SqlCon.java
 *
 * Created on 15 de febrebro de 2009, 17:41 AM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */
import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 *
 * @author Kerberus Ingeniería LtdaU
 * @version 1.9
 * @email: mario.lenis@kerrberusing.com
 */
public class SqlCon {
    private String HOST, USER, PSW, DB;
    private Connection con;
    public boolean onLine = false;
    
    /** Creates a new instance of SqlCon */
    public SqlCon(String HOST, String USER, String PSW, String DB) {
        this.HOST = HOST;
        this.USER = USER;
        this.PSW = PSW;
        this.DB = DB;
        try{
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        }catch(Exception e){}
        if (conectar(HOST,USER,PSW,DB)) {
            System.out.println("Conexion exitosa");
            onLine = true;
        }
    }
        
    public boolean conectar(String HOST, String USER, String PSW, String DB){
        try{
            con = DriverManager.getConnection("jdbc:mysql://"+HOST+"/"+DB+"?user="+USER+"&password="+PSW); 
            return true;
        }catch(SQLException e){
            System.out.println("Imposible conectarse a la base de datos");
            System.out.println(e.getMessage());
            System.out.println("SQLState: " + e.getSQLState());
            System.out.println("VendorError: " + e.getErrorCode());
        }
        return false;
    }
    
    public boolean execConsulta(String SQLstr){        
        try{
            if (con.isClosed()) 
                conectar(HOST,USER,PSW,DB);        
            Statement stmt = con.createStatement();
            stmt.execute(SQLstr);
            stmt.close();
            return true;
        }catch(SQLException e){
            System.out.println(e.getMessage());
        }
        return false;
    }
    
    public ResultSet execQuery(String SQLstr){        
        ResultSet rs = null;
        try{
            if (con.isClosed()) 
                conectar(HOST,USER,PSW,DB);        
                Statement stmt = con.createStatement();
            rs = stmt.executeQuery(SQLstr);
            return rs;
        }catch(Exception e){
            System.out.println(e.getMessage());
        }
        return null;
    }

    public boolean cerrarConexion(){
        try {
            if (!con.isClosed()) {
                con.close();
                con = null;
                return true;
            }
        } catch (SQLException ex) {
            Logger.getLogger(SqlCon.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }
}
