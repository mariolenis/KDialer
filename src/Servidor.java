import java.io.*;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
//import org.apache.commons.httpclient.params.HttpMethodParams;

/**
 *
 * @author Kailu Mario
 * @email: mario.lenis@kerberusing.com
 */
        
public class Servidor implements java.lang.Runnable{
    
    // Atributos
    Thread manejador        = null;
    Thread sms              = null;
    Thread mails            = null;
    Thread popMail          = null;
    SqlCon connectorCalls   = null;
    SqlCon connectorSMS     = null;
    SqlCon connectorMAIL    = null;
    SqlCon connectorPOP     = null;
    ResultSet resultQueue   = null;
    ResultSet resultSMS     = null;
    ResultSet resultMail    = null;
    String proveedor        = "";
    String prefijo          = "";
    String tech             = "";
    String proveedorSMS     = "";
    String url              = "";
    String techSMS          = "";
    String estRespuesta     = "ENVIADO";
    String smtp             = "";
    String puerto           = "";
    String smtpUser         = "";
    String smtpPwd          = "";
    String remitente        = "";
    String emailRtt         = "";


    /*private Connection con  = null;
    private String HOST, USER, PSW, DB;*/
    
    // Constructor
    public Servidor(){

        /*this.HOST   = "localhost"; 69.64.43.205
        this.USER   = "kerberus";
        this.PSW    = "srtel01";
        this.DB     = "kerberus";*/
        
        connectorCalls  = new SqlCon("localhost", "contactnow", "seraph", "kerberus");
        connectorSMS    = new SqlCon("localhost", "contactnow", "seraph", "kerberus");
        connectorMAIL   = new SqlCon("localhost", "contactnow", "seraph", "kerberus");
        connectorPOP    = new SqlCon("localhost", "contactnow", "seraph", "kerberus");

        //connector = new SqlCon("localhost", "root", "seraph", "kerberusdialer");
        manejador   = new Thread(this);
        sms         = new Thread(this);
        mails       = new Thread(this);
        popMail     = new Thread(this);
        sms.setName("sms");
        mails.setName("mails");
        manejador.setName("eCalls");
        popMail.setName("PopMails");

        System.out.println("\nKerberus ContactNOW(R)\nIniciando servicios...[ dialer | SMS | mailist ]");
        manejador.start();
        //popMail.start();
        //mails.start();
        //sms.start();
        System.out.println("Servicios Iniciados.");
    }
    
    public void run() {                
        Thread hilo = Thread.currentThread();
        
        // <editor-fold defaultstate="collapsed" desc="Envio de LLamadas">
        while (hilo == manejador) {
            BufferedWriter escritor = null;
            try {
                resultQueue = connectorCalls.execQuery("SELECT * FROM campana WHERE estado='activa'");

                if (resultQueue != null && resultQueue.first()) {
                    do {
                        //Verificar si hay llamadas para hacer
                        ResultSet llamadas = connectorCalls.execQuery("SELECT COUNT(idNumero) AS cant FROM numero WHERE estado='enEspera' AND fecha < NOW() AND idCampana='" + resultQueue.getString("idCampana") + "'");

                        if (llamadas != null && llamadas.first() && llamadas.getInt("cant") > 0 ) { // Si hay llamadas para hacer
                            llamadas.close();

                            //Selección del proveedor con el que se harán las llamadas.
                            ResultSet setProveedor = connectorCalls.execQuery("SELECT * FROM proveedor WHERE (tecnologia='SIP' OR tecnologia='IAX2') AND seleccionado=1");
                            if (setProveedor != null && setProveedor.first()) {
                                proveedor = setProveedor.getString("nombre");
                                prefijo = setProveedor.getString("prefijo");
                                tech = setProveedor.getString("tecnologia");
                            } else
                                System.out.println("ATENCION: No hay proveedor seleccionado o el actual no es válido.\n");

                            setProveedor.close();
                            setProveedor = null;

                            File enCurso = new File("/var/spool/asterisk/outgoing/");
                            FilenameFilter filtro = new FilenameFilter() {

                                public boolean accept(File enCurso, String name) {
                                    try {
                                        return name.endsWith(resultQueue.getString("cola") + resultQueue.getString("idCampana") + ".call");
                                    } catch (SQLException ex) {
                                        Logger.getLogger(Servidor.class.getName()).log(Level.SEVERE, null, ex);
                                    }
                                    return false;
                                }
                            };
                            int cantidad = Integer.parseInt(resultQueue.getString("limite")) - enCurso.list(filtro).length;
                            enCurso = null;
                            llamadas = connectorCalls.execQuery("SELECT * FROM numero WHERE estado='enEspera' AND fecha < NOW() AND idCampana='" + resultQueue.getString("idCampana") + "' ORDER BY numero LIMIT " + cantidad);

                            if (llamadas != null && llamadas.first()) {
                                do {
                                    String archivo = "/var/spool/asterisk/outgoing/" + llamadas.getString("numero") + "-" + resultQueue.getString("cola") + resultQueue.getString("idCampana") + ".call";
                                    FileWriter tempFile = new FileWriter(archivo);
                                    escritor = new BufferedWriter(tempFile);
                                    if (llamadas.getString("numero").length() > 8)
                                        escritor.write("Channel: SIP/Ligatel/0057" + llamadas.getString("numero"));
                                    else
                                        escritor.write("Channel: " + tech + "/" + proveedor + "/" + prefijo + llamadas.getString("numero"));
                                    escritor.newLine();
                                    escritor.write("Callerid: 5725240626");
                                    escritor.newLine();
                                    escritor.write("MaxRetries: 0");
                                    escritor.newLine();
                                    escritor.write("RetryTime: 20");
                                    escritor.newLine();
                                    if (llamadas.getString("numero").length() > 8)
                                        escritor.write("WaitTime: 30");
                                    else
                                        escritor.write("WaitTime: 20");
                                    escritor.newLine();
                                    escritor.write("Context: " + resultQueue.getString("contexto"));
                                    escritor.newLine();
                                    escritor.write("Extension: 611");
                                    escritor.newLine();
                                    escritor.write("Priority: 1");
                                    escritor.newLine();
                                    escritor.newLine();
                                    escritor.write("Set: idCall=" + llamadas.getString("numero") + "-" + resultQueue.getString("cola") + "-" + resultQueue.getString("mensaje") + "-" + llamadas.getString("fecharecordatorio") + "-" + llamadas.getString("idNumero"));

                                    // finalización.
                                    escritor.flush();
                                    escritor.close();
                                    escritor = null;
                                    tempFile.close();
                                    tempFile = null;

                                    // Actualizar el estado de la llamada a "Realizando"
                                    connectorCalls.execConsulta("UPDATE numero SET estado='Realizando' WHERE idNumero='" + llamadas.getString("idNumero") + "'");

                                } while (llamadas.next());
                            }
                            llamadas.close();
                            llamadas = null;
                        } 
                    } while (resultQueue.next());
                }
                resultQueue.close();
                resultQueue = null;
                Thread.sleep(1000);
            } catch (Exception ex) {
                System.out.println(ex.getMessage() + "\n" + ex.getCause());
                Logger.getLogger(Servidor.class.getName()).log(Level.SEVERE, null, ex);                
            }
            System.gc();
        }// </editor-fold>

        // <editor-fold defaultstate="collapsed" desc="Envio de SMS">
        while (hilo == sms) {
                try {
                    estRespuesta = "FALLIDO";
                    resultSMS = connectorSMS.execQuery("SELECT numerosms.idMensaje, mensaje, mensaje.usuario, idusuario FROM numerosms, mensaje, campanasms WHERE estado='ENESPERA' AND (numerosms.idMensaje = mensaje.idMensaje AND mensaje.usuario = campanasms.usuario) AND fecha < NOW() GROUP BY numerosms.IdMensaje");
                    if (resultSMS != null && resultSMS.first()) {
                        do {
                            //ResultSet smsNumero = connectorSMS.execQuery("SELECT numero, idNumero, nombre FROM numerosms WHERE estado='ENESPERA' AND idMensaje='" + resultSMS.getString("idMensaje") + "' AND numero NOT BETWEEN '310%' AND '315%' AND numero NOT BETWEEN '320%' AND '323%' LIMIT 100");
                            ResultSet smsNumero = connectorSMS.execQuery("SELECT numero, idNumero, nombre FROM numerosms WHERE estado='ENESPERA' AND idMensaje='" + resultSMS.getString("idMensaje") + "' LIMIT 100");
                            
                            if (smsNumero != null && smsNumero.first()) {
                                //Selección del proveedor con el que se enviaran los SMS.
                                ResultSet setProveedor = connectorSMS.execQuery("SELECT * FROM proveedor where tecnologia='SMS' AND seleccionado=1");

                                String proveedorSMSh = "", urlh = ""; // Proveedor por defecto
                                if (setProveedor != null && setProveedor.first()) {
                                    proveedorSMSh = setProveedor.getString("nombre");
                                    urlh = setProveedor.getString("prefijo");
                                } else 
                                    System.out.println("ATENCION: No hay proveedor seleccionado o el actual no es válido.\n");
                                setProveedor.close();
                                setProveedor = null;

                                do {

                                    StringBuilder data = new StringBuilder();
                                    String fromName = resultSMS.getString("idusuario").toUpperCase();
                                    int prefijoDest = 0;

                                    try {
                                        prefijoDest = Integer.parseInt(smsNumero.getString("numero").substring(0, 3));
                                    }catch (Exception s){
                                        connectorSMS.execConsulta("DELETE FROM numero WHERE numero='"+ smsNumero.getString("numero") +"'");
                                        break;
                                    }

                                    if ((prefijoDest >= 310 && prefijoDest <= 314) || (prefijoDest >= 320 && prefijoDest <= 322)){
                                        /*proveedorSMS = "mobile-sms";
                                        url = "http://217.118.27.5/bulksms/bulksend.go?";
                                        data.append("username=mario.lenis@kerberus.com.co&");
                                        data.append("password=lerez__09&");
                                        data.append("msgtext=" + URLEncoder.encode(resultSMS.getString("mensaje").replace("<var1>", smsNumero.getString("nombre").trim()).replaceAll("á", "à"), "ISO-8859-1") + "&");
                                        data.append("originator=" + fromName + "&");
                                        data.append("charset=8&");
                                        data.append("phone=57" + smsNumero.getString("numero"));*/
                                        proveedorSMS = "elibom";
                                        url = "http://www.elibom.com/http/sendmessage?";
                                        data.append("username=mario.lenis@kerberusing.com&");
                                        data.append("password=kerberus__11&");
                                        data.append("message=" + URLEncoder.encode(resultSMS.getString("mensaje").replace("<var1>", smsNumero.getString("nombre").trim()).replaceAll("á", "à"), "ISO-8859-1") + "&");
                                        data.append("to=57");
                                        data.append(smsNumero.getString("numero"));
                                        /*proveedorSMS = "mobile-sms";
                                        url = "http://217.118.27.5/bulksms/bulksend.go?";
                                        data.append("username=mario.lenis@kerberusing.com&");
                                        data.append("password=BD21D612&");
                                        data.append("msgtext=" + URLEncoder.encode(resultSMS.getString("mensaje").replace("<var1>", smsNumero.getString("nombre").trim()).replaceAll("á", "à"), "ISO-8859-1") + "&");
                                        data.append("originator=" + fromName + "&");
                                        data.append("charset=8&");
                                        data.append("phone=57" + smsNumero.getString("numero"));*/
                                    }
                                    else {
                                        proveedorSMS = proveedorSMSh;
                                        url = urlh;

                                        if (proveedorSMS.equals("infobip")) {
                                            data.append("user=kerberus&");
                                            data.append("password=kerb398&");
                                            data.append("sender=" + fromName + "&");
                                            data.append("IsFlash=0&");
                                            data.append("SMSText=" + URLEncoder.encode(resultSMS.getString("mensaje").replace("<var1>", smsNumero.getString("nombre").trim()), "UTF-8") + "&");
                                            data.append("GSM=57" + smsNumero.getString("numero"));
                                        } else if (proveedorSMS.equals("bulksms")) {
                                            data.append("username=kerberus&");
                                            data.append("password=lerez09&");
                                            data.append("message=" + URLEncoder.encode(resultSMS.getString("mensaje").replace("<var1>", smsNumero.getString("nombre").trim()), "UTF-8") + "&");
                                            data.append("want_report=1&");
                                            data.append("sender=" + fromName + "&");
                                            data.append("routing_group=3&");
                                            data.append("msisdn=57" + smsNumero.getString("numero").replace("<var1>", smsNumero.getString("nombre")).trim());
                                        } else if (proveedorSMS.equals("clickatell")) {
                                            data.append("user=kerberus&");
                                            data.append("password=lerez__09&");
                                            data.append("text=" + URLEncoder.encode(resultSMS.getString("mensaje").replace("<var1>", smsNumero.getString("nombre")).trim(), "UTF-8") + "&");
                                            data.append("api_id=3166394&");
                                            data.append("from=" + fromName + "&");
                                            data.append("to=57" + smsNumero.getString("numero"));
                                        } else if (proveedorSMS.equals("42it")) {
                                            data.append("username=Kerberus&");
                                            data.append("password=lerez__09&");
                                            data.append("message=" + URLEncoder.encode(resultSMS.getString("mensaje").replace("<var1>", smsNumero.getString("nombre").trim()), "ISO-8859-1") + "&");
                                            data.append("from=" + fromName + "&");
                                            data.append("route=G1&");
                                            data.append("to=57" + smsNumero.getString("numero"));
                                        } else if (proveedorSMS.equals("mobile-sms")) {
                                            data.append("username=mario.lenis@kerberusing.com&");
                                            data.append("password=BD21D612&");
                                            data.append("msgtext=" + URLEncoder.encode(resultSMS.getString("mensaje").replace("<var1>", smsNumero.getString("nombre").trim()).replaceAll("á", "à"), "ISO-8859-1") + "&");
                                            data.append("originator=" + fromName + "&");
                                            data.append("charset=8&");
                                            data.append("phone=57" + smsNumero.getString("numero"));
                                        }
                                        else if (proveedorSMS.equals("elibom")) {
                                            data.append("username=mario.lenis@kerberusing.com&");
                                            data.append("password=kerberus__11&");
                                            data.append("message=" + URLEncoder.encode(resultSMS.getString("mensaje").replace("<var1>", smsNumero.getString("nombre").trim()).replaceAll("á", "à"), "ISO-8859-1") + "&");
                                            data.append("to=57");
                                            data.append(smsNumero.getString("numero"));
                                        }
                                    }
                                    // <editor-fold defaultstate="collapsed" desc="ENVÍO & ACTUALIZACIÓN DE ESTADO">
                                        HttpClient cliente = new HttpClient();
                                        HttpMethod metodo = new GetMethod(url + data.toString());

                                        try {
                                            cliente.executeMethod(metodo);
                                            String respuesta = metodo.getResponseBodyAsString();
                                            respuesta = respuesta.replace("\n", "").toUpperCase();

                                            if (proveedorSMS.equals("bulksms")) {
                                                String idConsulta = respuesta.substring((respuesta.lastIndexOf("|") + 1));

                                                metodo = new GetMethod("http://bulksms.vsms.net:5567/eapi/status_reports/get_report/2/2.0?username=kerberus&password=lerez09&batch_id=" + idConsulta);
                                                cliente.executeMethod(metodo);
                                                respuesta = metodo.getResponseBodyAsString();

                                                while (true) {
                                                    metodo = new GetMethod("http://bulksms.vsms.net:5567/eapi/status_reports/get_report/2/2.0?username=kerberus&password=lerez09&batch_id=" + idConsulta);
                                                    cliente.executeMethod(metodo);
                                                    respuesta = metodo.getResponseBodyAsString();
                                                    respuesta = respuesta.replace("\n", "");
                                                    int cod = Integer.parseInt(respuesta.substring((respuesta.lastIndexOf("|") + 1)));

                                                    if (cod > 9 && cod < 13) {
                                                        respuesta = "OK";
                                                        break;
                                                    }
                                                }
                                            }
                                            else if(proveedorSMS.equals("elibom") && respuesta.startsWith("0"))                                                
                                                respuesta = "OK";
                                            
                                            if (((int) (respuesta.charAt(0)) > 0 || respuesta.startsWith("1,") || respuesta.startsWith("ID") || respuesta.startsWith("OK")) && (!respuesta.startsWith("-") && !respuesta.startsWith("ERR")))
                                                estRespuesta = "ENVIADO";
                                            else
                                                estRespuesta = "FALLIDO";
                                            System.out.println(estRespuesta + " " + smsNumero.getString("numero"));
                                        } catch (Exception k) {
                                            System.out.println(k.getMessage());
                                        } finally {
                                            // Actualizar el estado.
                                            metodo.releaseConnection();
                                            connectorSMS.execConsulta("UPDATE numerosms SET estado='" + estRespuesta + "' WHERE idNumero='" + smsNumero.getString("idNumero") + "'");
                                            updateSdr(smsNumero.getString("numero"), resultSMS.getString("idMensaje"), resultSMS.getString("usuario"), estRespuesta);
                                        }// </editor-fold>

                                } while (smsNumero.next());
                            }
                            smsNumero.close();
                            smsNumero = null;
                        } while (resultSMS.next());
                    }
                    resultSMS.close();
                    resultSMS = null;
                    Thread.sleep(1000);
                } catch (Exception ex) {
                    System.out.println(ex.getMessage() + "\n" + ex.getCause());
                    Logger.getLogger(Servidor.class.getName()).log(Level.SEVERE, null, ex);
                }
                System.gc();
        }// </editor-fold>

        // <editor-fold defaultstate="collapsed" desc="Mailist">
        while (hilo == mails) {
            try {
                // Identificar las campañas Activas = estado = 'enespera'
                resultMail = connectorMAIL.execQuery("SELECT campanaMail.*, flayer.img FROM campanaMail, flayer WHERE (estado='enespera' OR estado='realizando') AND fEnvio < NOW() AND campanaMail.idflayer = flayer.idflayer");
                if (resultMail != null && resultMail.first()){                                        
                    do {
                        String defaultState = "realizada";
                        String idServer = "-1";
                        
                        // Actualización del estado de la campaña en curso.
                        connectorMAIL.execConsulta("UPDATE campanaMail SET estado='realizando' WHERE id='" + resultMail.getString("id") + "'");
                        // Datos del proveedor de correo.
                        ResultSet setsmtp = connectorMAIL.execQuery("SELECT * FROM mailist WHERE usuario='" + resultMail.getString("usuario") + "'");

                        if (setsmtp != null && setsmtp.first() && setsmtp.getString("smtp").contains("contactnow")){                            

                            ResultSet contactnow = cargarServer();                            
                            while(contactnow == null){
                                Thread.sleep(60*1000); // Si no hay servidor disponible se duerme 1 minuto
                                contactnow = cargarServer();
                            }

                            smtp        = contactnow.getString("pop");
                            puerto      = "25";
                            smtpUser    = contactnow.getString("user");
                            smtpPwd     = contactnow.getString("password");
                            emailRtt    = contactnow.getString("user");
                            idServer    = contactnow.getString("idserver");

                        }else {
                            smtp = setsmtp.getString("smtp");
                            puerto = setsmtp.getString("puerto");
                            smtpUser = setsmtp.getString("usr");
                            smtpPwd = setsmtp.getString("pwd");
                            emailRtt  = setsmtp.getString("emailremitente");                            
                        }

                        remitente = setsmtp.getString("remitente");
                        int cantidad = setsmtp.getInt("cantidad");
                        int offset   = resultMail.getInt("offset");

                        // Contar la cantidad de los yá enviados.
                        setsmtp = connectorMAIL.execQuery("SELECT COUNT(idmdr) AS cantSent FROM mdr WHERE usuario='" + resultMail.getString("usuario") + "' AND estado != 'FALLIDO'");
                        int enviados = 0;
                        try{
                            setsmtp.first();
                            enviados = setsmtp.getInt("cantSent");
                            setsmtp.close();
                            setsmtp = null;
                        }catch(Exception g){
                            return;
                        }
                        
                        // Cargar el mensaje y limitar la cantidad de correos con [cantidad] - enviados.
                        if (cantidad - enviados >= 0) { // seguir sino terminar
                            
                            // Leer los destinatarios.
                            if ((cantidad - enviados) - 25 < 0)
                                cantidad = 25 - (cantidad - enviados);
                            else
                                cantidad = 25;
                            ResultSet campanaMails = connectorMAIL.execQuery(resultMail.getString("clientes") + " LIMIT " + offset + "," + cantidad);
                            System.out.println(resultMail.getString("clientes") + " LIMIT " + offset + "," + cantidad);
                            
                            connectorMAIL.execConsulta("UPDATE campanaMail SET offset = " + (25 + offset) + " WHERE id = '" + resultMail.getString("id") + "'");
                            java.util.Date f = new java.util.Date(new java.util.Date().getTime());
                            
                            if (campanaMails != null && campanaMails.first()){
                                Properties props = System.getProperties();
                                javax.mail.Authenticator auth = new PopupAuthenticator(smtpUser, smtpPwd);
                                props.put("mail.smtp.host", smtp);
                                props.put("mail.smtp.auth", "true");
                                props.put("mail.smtp.port", puerto);
                                Session session = Session.getInstance(props, auth);

                                do {
                                    MimeMessage message = new MimeMessage(session);
                                    message.setFrom(new InternetAddress(emailRtt, remitente));
                                    message.setSubject(campanaMails.getString("nombre") + ": " +resultMail.getString("asunto"));
                                    message.setSentDate(f);
                                    message.addRecipient(Message.RecipientType.TO, new InternetAddress(campanaMails.getString("email"), campanaMails.getString("nombre")));

                                    String emailTemp = campanaMails.getString("email");
                                    if (campanaMails.getString("email").equals("empresa"))
                                        emailTemp = emailTemp.substring(emailTemp.indexOf("@") - 2);

                                    message.setContent(cargarCuerpo(resultMail.getString("id"), resultMail.getString("usuario"), resultMail.getString("idflayer"), campanaMails.getString("email"), campanaMails.getString("nombre"), resultMail.getString("img"), resultMail.getString("asunto"), remitente, resultMail.getString("url")), "text/html");
                                    try {                                        
                                        Transport.send(message);
                                        System.out.println("Enviado " + idServer);
                                        connectorMAIL.execConsulta("INSERT INTO mdr (email, idCampana, usuario, estado, messageid) VALUES ('"+ emailTemp.toLowerCase() +"', '" + resultMail.getString("id") + "', '"+ resultMail.getString("usuario") +"', 'enviado', '" + message.getMessageID() + "')");
                                        connectorMAIL.execConsulta("UPDATE mailistservers SET offset = offset+1 WHERE idserver = " + idServer);
                                    }catch(Exception m){
                                        System.out.println("Error " + m.getMessage());
                                        defaultState = "fallida";
                                        connectorMAIL.execConsulta("INSERT INTO mdr (email, idCampana, usuario, estado, messageid) VALUES ('"+ emailTemp.toLowerCase() +"', '" + resultMail.getString("id") + "', '"+ resultMail.getString("usuario") +"', 'fallido', '" + message.getMessageID() + "')");                                        
                                    }
                                    message = null;
                                } while (campanaMails.next());

                                campanaMails.close();
                                campanaMails = null;
                            }
                            else
                                connectorMAIL.execConsulta("UPDATE campanaMail SET estado='"+ defaultState +"' WHERE id='" + resultMail.getString("id") + "'");
                        }else{
                            System.out.println((cantidad - enviados));
                            connectorMAIL.execConsulta("UPDATE campanaMail SET estado='No hay disponibles' WHERE id='" + resultMail.getString("id") + "'");
                        }
                    } while(resultMail.next());
                }
                resultMail.close();
                resultMail = null;
                Thread.sleep(10000);
            } catch (Exception ex) {
                System.out.println(ex.getMessage() + "\n" + ex.getCause());
                Logger.getLogger(Servidor.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.gc();
        }// </editor-fold>

        // <editor-fold defaultstate="collapsed" desc="popMail">
        while (hilo == popMail) {
            InputStream stream = null;
            try {
                ResultSet popDemon = connectorPOP.execQuery("SELECT * FROM mailistservers");
                if (popDemon != null && popDemon.first()){
                    do{
                        Properties props = System.getProperties();
                        Session session = Session.getInstance(props);
                        Store store = session.getStore("pop3");
                        //store.connect("mail.contactnow01.info", "boletines@contactnow01.info", "boletines10");
                        store.connect(popDemon.getString("pop"), popDemon.getString("user"), popDemon.getString("password"));
                        Folder folder = store.getFolder("inbox");
                        folder.open(Folder.READ_WRITE);
                        Message[] message = folder.getMessages();
                        for (int i = 0; i < message.length; i++) {
                            if (message[i].getSize() < 20 * 1024) {
                                stream = message[i].getInputStream();
                                StringBuilder txt = new StringBuilder();
                                while (stream.available() != 0) {
                                    txt.append((char) stream.read());
                                }
                                String cadTemp = txt.toString();
                                String[] lineas = cadTemp.split("\n");
                                String email = "";
                                String messageId = "";
                                for (int j = 0; j < lineas.length; j++) {
                                    if (lineas[j].startsWith("To:") && lineas[j].contains("<")) {
                                        email = lineas[j].substring(lineas[j].indexOf("<") + 1, lineas[j].length() - 2);
                                    } else if (lineas[j].startsWith("To:")) {
                                        System.out.println(lineas[j]);
                                    }
                                    if (lineas[j].startsWith("Message-ID:") && lineas[j].contains("<")) {
                                        messageId = lineas[j].substring(lineas[j].indexOf("<"), lineas[j].length() - 1);
                                    } else if (lineas[j].startsWith("Message-ID:")) {
                                        System.out.println(lineas[j]);
                                    }
                                }
                                if (!email.trim().equals("") && !messageId.trim().equals("") && connectorPOP.execConsulta("UPDATE mdr SET estado='fallido' WHERE email='" + email.trim().toLowerCase() + "' AND messageid='" + messageId.trim() + "'")) {
                                    connectorPOP.execConsulta("DELETE FROM mail WHERE email='" + email.trim().toLowerCase() + "'");
                                }
                                stream.close();
                                stream = null;
                            }
                            message[i].setFlag(Flags.Flag.DELETED, true);
                        }
                        message = null;
                        folder.close(true);
                        store.close();
                        store = null;
                    }while(popDemon.next());
                }
            } catch (SQLException ex) {
                Logger.getLogger(Servidor.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(Servidor.class.getName()).log(Level.SEVERE, null, ex);
            } catch (MessagingException ex) {
                System.out.println(ex.getMessage() + "\n" + ex.getCause());
                Logger.getLogger(Servidor.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                try {
                    System.gc();
                    if (stream != null)
                        stream.close();
                    Thread.sleep((5*60*1000));
                } catch (InterruptedException ex) {
                    Logger.getLogger(Servidor.class.getName()).log(Level.SEVERE, null, ex);
                } catch (IOException ex) {
                    Logger.getLogger(Servidor.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }// </editor-fold>
    }

    // <editor-fold defaultstate="collapsed" desc="FUNCIONES DE DB">
    /*
    private boolean conectarDB(String HOST, String USER, String PSW, String DB) {
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            con = null;
            con = DriverManager.getConnection("jdbc:mysql://" + HOST + "/" + DB + "?user=" + USER + "&password=" + PSW);
            System.out.println("Conexión Exitosa");
            return true;
        } catch (SQLException ex) {
            Logger.getLogger(Servidor.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Servidor.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            Logger.getLogger(Servidor.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(Servidor.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }

    public boolean execConsulta(String SQLstr) {
        try {
            if (con.isClosed()) {
                conectarDB(HOST, USER, PSW, DB);
            }
            Statement stmt = con.createStatement();
            stmt.execute(SQLstr);
            stmt.close();
            return true;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return false;
    }

    public ResultSet execQuery(String SQLstr) {

        try {
            if (con.isClosed())
                conectarDB(HOST, USER, PSW, DB);
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(SQLstr);
            return rs;
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }*/
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="cargarServidorMAILIST">
    private ResultSet cargarServer() {

        int maxEnvios = 300;
        while (true) {
            try {
                // Búsqueda de un servidor que tenga menos de 500 envíos y que la última fecha al alcanzar los maxEnvios emails haya sido hace mas de una hora.
                ResultSet contactnow = connectorMAIL.execQuery("SELECT * FROM mailistservers WHERE offset < "+ maxEnvios +" AND fechaCambio < DATE_SUB(NOW(),INTERVAL 1 HOUR) ORDER BY fechaCambio LIMIT 1");
                if (contactnow != null && contactnow.first()){
                    int offsetContactNow = Integer.parseInt(contactnow.getString("offset"));
                    String idServer = contactnow.getString("idserver");
                    System.out.println(contactnow.getString("user"));

                    if ((offsetContactNow + 25) > maxEnvios)
                        connectorMAIL.execConsulta("UPDATE mailistservers SET offset=0, fechaCambio=NOW() WHERE idserver=" + idServer);
                    else
                        return contactnow;
                }else
                    return null;
            } catch (SQLException ex) {
                Logger.getLogger(Servidor.class.getName()).log(Level.SEVERE, null, ex);
            }
        }        
    }// </editor-fold>

    private String quitarTildes(String valor){
        valor = valor.replace("ñ","N;");
	valor = valor.replace("Ñ","N");
	valor = valor.replace("á","a");
	valor = valor.replace("é","e");
	valor = valor.replace("í","i");
	valor = valor.replace("ó","o");
	valor = valor.replace("ú","u");
	valor = valor.replace("Á","A");
	valor = valor.replace("É","E");
	valor = valor.replace("Í","I");
	valor = valor.replace("Ó","O");
	valor = valor.replace("Ú","U");
	valor = valor.replace("'","&#8217;");

        return valor;
    }

    private boolean updateSdr(String numero, String idCampana, String usuario, String estado){
        if(connectorSMS.execConsulta("INSERT INTO sdr (numero, idCampana, usuario, estado) VALUES ('"+numero+"','"+idCampana+"','"+usuario+"','"+estado+"')"))
            return true;
        return false;
    }

    private String fixTildes(String valor){
        valor = valor.replace("ñ","&ntilde;");
	valor = valor.replace("Ñ","&Ntilde;");
	valor = valor.replace("á","&aacute;");
	valor = valor.replace("é","&eacute;");
	valor = valor.replace("í","&iacute;");
	valor = valor.replace("ó","&oacute;");
	valor = valor.replace("ú","&uacute;");
	valor = valor.replace("Á","&Aacute;");
	valor = valor.replace("É","&Eacute;");
	valor = valor.replace("Í","&Iacute;");
	valor = valor.replace("Ó","&Oacute;");
	valor = valor.replace("Ú","&Uacute;");
	valor = valor.replace("'","&#8217;");

        return valor;
    }

    // <editor-fold defaultstate="collapsed" desc="CuerpoHTML">
    private String cargarCuerpo(String idCampana, String idUsuario, String id, String email, String nombre, String img, String asunto, String remitente, String urlR) {
        StringBuilder body = new StringBuilder();

        body.append("<html><head><meta http-equiv=\"Content-Type\" content=\"text/html; charset=iso-8859-1\" /></head>");
        body.append("<body><center>");

        body.append("<div style=\"width:800px; text-align:center; border:1px solid #CCCCCC\">");
        body.append("<table border=\"0\" cellpadding=\"6\" cellspacing=\"0\">");
        body.append("<tr><td>");
        body.append("<strong>");
        body.append("<div style=\"font-size:12px; text-align:left; border:1px solid white; font-family:Verdana, Arial, Helvetica, sans-serif; \">");
        body.append("Apreciado(a): ");
        body.append(nombre);
        body.append("</div>");
        body.append("<br>");
        body.append("<div style=\"font-size:12px; text-align:center; border:1px solid white; font-family:Verdana, Arial, Helvetica, sans-serif; \">");
        body.append(fixTildes(asunto));
        body.append("</div></strong>");
        body.append("<br /><br />");
        body.append("<div align=\"center\">");
        body.append("<a href=\"" + urlR + "\" target=\"_blank\">");
        body.append("<img src=\"http://www.contactnow10.info/Websites/KDquery/" + img + "\" border=\"0\" >");
//        body.append("<img src=\"http://www.contactnow10.info/Websites/KDquery/mailist/hh.php?file=" + img + "&amp;id=" + idCampana + "&amp;email=" + email + "\" border=\"0\"  />");
        body.append("</a>");
        body.append("</div>");
        body.append("</td></tr>");
        body.append("</tr><td>");
        body.append("<DIV style=\"text-align:justify; BORDER: #cccccc 1px solid; BACKGROUND: #e4e4e4\">");
        body.append("<table width=\"100%\" cellpadding=\"6\">");
        body.append("<tr><td style=\"FONT-SIZE: 10px; COLOR: #666666; FONT-FAMILY: Verdana, Arial, Helvetica, sans-serif; text-align:justify\">");
        body.append("Este correo fu&eacute; enviado a&nbsp;<B>");
        body.append(email);
        body.append("</B> La informaci&oacute;n contenida en ");
        body.append("esta transmisi&oacute;n de correo electr&oacute;nico est&aacute; destinado por <STRONG>" + fixTildes(remitente));
        body.append("</STRONG> para el uso del nombre de la persona o entidad a la que va ");
        body.append("dirigida y puede contener informaci&oacute;n privilegiada o confidencial. Si usted ha ");
        body.append("recibido esta transmisi&oacute;n de correo electr&oacute;nico por error, por favor, borre de ");
        body.append("su sistema sin copiar o reenviar, y notificar al remitente del mensaje de error ");
        body.append("por la respuesta de e-mail o por tel&eacute;fono, por lo que los registros de la ");
        body.append("direcci&oacute;n del remitente se puede corregir.<BR><BR>");
        body.append("<DIV align=\"right\">Si desea ser dado de bajo <A ");
        body.append("href=\"http://www.contactnow10.info/Websites/KDquery/mailist/?usr=" + idUsuario + "&amp;id=" + idCampana + "&amp;opt=down&amp;email=" + email + "\"><STRONG>escala tu solicitud</STRONG></A> ");
        body.append(" o responde este correo con el subject <STRONG>SALIR</STRONG> </DIV>");
        body.append("</td></tr>");
        body.append("</table>");
        body.append("</DIV>");
        body.append("<DIV style=\"FONT-SIZE: 11px; COLOR: #666666; FONT-FAMILY: Verdana, Arial, Helvetica, sans-serif; TEXT-ALIGN: center\">");
        body.append("<br>");
        body.append("<STRONG>&copy;2011 Promec Ingenier&iacute;a S.A.S.</STRONG><BR>Todos los derechos reservados. ");
        body.append("</DIV>");        
        body.append("</td></tr>");
        body.append("</table>");
        body.append("<div style=\"font-family:Verdana, Arial, Helvetica, sans-serif; font-size:11px\">");
        body.append("Si tienes dificultad para ver este correo, por favor ");
        body.append("<a href=\"http://www.contactnow10.info/Websites/KDquery/preview.php?");
        body.append("camp=" + idUsuario + "&amp;id=" + id + "&amp;email=" + email + "&amp;nombre=" + nombre + "&amp;remitente=" + fixTildes(remitente) + "&amp;asunto=" + fixTildes(asunto) + "\">siguenos</a><br /><br /></div>");
        body.append("</div></center></body></html>");

        return body.toString();
    }// </editor-fold>
    
    // <editor-fold defaultstate="collapsed" desc="MD5">
    public static String encryptMD5(String code) {        
        try {            
            MessageDigest md = MessageDigest.getInstance("MD5");            
            byte[] input = code.getBytes(); //"UTF8");            
            input = md.digest(input);            
            code = toHexadecimal(input); //new String(input,"UTF8");                
            return code;            
        } catch (Exception e) {            
            
            return code;            
        }        
        
    }    

    private static String toHexadecimal(byte[] datos) {        
        String resultado = "";        
        ByteArrayInputStream input = new ByteArrayInputStream(datos);        
        String cadAux;        
        boolean ult0 = false;        
        int leido = input.read();        
        while (leido != -1) {            
            cadAux = Integer.toHexString(leido);            
            if (cadAux.length() < 2) { //Hay que aÒadir un 0   
                resultado += "0";                
                if (cadAux.length() == 0) {
                    ult0 = true;                    
                    
                }
            } else {
                ult0 = false;
            }            
            resultado += cadAux;            
            leido = input.read();            
        }        
        if (ult0)//quitamos el 0 si es un caracter aislado  
        {
            resultado =
                    resultado.substring(0, resultado.length() - 2) + resultado.charAt(resultado.length() - 1);            
            
        }
        return resultado;        
    }// </editor-fold>

    class PopupAuthenticator extends javax.mail.Authenticator {
        String username;
        String password;

        public PopupAuthenticator(String username,String password){
            this.username=username;
            this.password=password;
        }

        @Override
        public javax.mail.PasswordAuthentication getPasswordAuthentication() {
            return new javax.mail.PasswordAuthentication(username,password);
        }
    }

    public static void main(String args[]){
        new Servidor();
    }

}

