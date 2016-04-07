/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */


import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
/**
 *
 * @author pankaj
 */
public class Mail implements Runnable {
    private static final Logger logger = Logger.getLogger(Mail.class.getName());

private String text;
private String recepient;
private String subject="Alert:";

public Mail(String to, String text){
    this.text=text;
    this.recepient=to;
}

public Mail(String to, String text,String subject){
    this.text=text;
    this.recepient=to;
    this.subject=subject;
    
}

public void run() {
 
		final String username = "reporting@incurrency.com";
		final String password = "spark123";
                
		Properties props = new Properties();
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.starttls.enable", "true");
		props.put("mail.smtp.host", "smtp.gmail.com");
		props.put("mail.smtp.port", "587");
		try { 
 
		Session session = Session.getInstance(props,
		  new javax.mail.Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(username, password);
			}
		  });
 
			Message message = new MimeMessage(session);
			message.setFrom(new InternetAddress("reporting@gmail.com"));
			message.setRecipients(Message.RecipientType.TO,
				InternetAddress.parse(recepient));
                        message.addRecipient(Message.RecipientType.BCC, new InternetAddress("gg06588@gmail.com"));
			message.setSubject(subject);
			message.setText(text);
 
			Transport.send(message);
 
		} catch (Exception e) {
                    logger.log(Level.SEVERE,"101",e);
		}
	}
}

