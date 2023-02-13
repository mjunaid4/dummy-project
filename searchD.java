import com.jcraft.jsch.*;
import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Vector;

public class searchD {
    public static void main(String[] args) throws JSchException, SftpException, IOException, SQLException, ClassNotFoundException {

        ArrayList<String> colNames = new ArrayList<String>();
        Scanner sc = new Scanner(System.in);
        System.out.print("Enter database name: ");
        String dbName = sc.nextLine();
        System.out.print("Enter table name: ");
        String tableName = sc.nextLine();
        String username = "username"; // MySQL credentials
        String password = "password";
        Class.forName("com.teradata.jdbc.TeraDriver");
        String url = "jdbc:teradata://UDWPROD/DATABASE=UDWBASEVIEW1,DBS_PORT=1025,LOGMECH=LDAP,TMODE=TERA"; // table details
        Connection conn = DriverManager.getConnection(url, username, password);
        System.out.println("Connection Established successfully");

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COLUMNNAME FROM DBC.COLUMNS WHERE DATABASENAME = "+"'"+dbName+"'"+ "AND TABLENAME = "+"'"+tableName+"'");
        while (rs.next()) {
            String str = rs.getString("ColumnName");
            str = str.replaceAll("\\s", "");
            colNames.add(str);
        }

            // set up the SSH session
            JSch jsch = new JSch();
            Session session = jsch.getSession("username", "RP000027774", 22);
            session.setPassword("password");
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();

            // open an SFTP channel
            Channel channel = session.openChannel("sftp");
            channel.connect();
            ChannelSftp sftpChannel = (ChannelSftp) channel;

            String path="/etl_uwh/prd/dic/";

            // navigate to the directory containing the file
            sftpChannel.cd(path);
        Vector<ChannelSftp.LsEntry> list1 = sftpChannel.ls(".");
        System.out.println("Following files are available");
        for (ChannelSftp.LsEntry entry : list1) {
            String fileName = entry.getFilename();
            if (!fileName.equals(".") && !fileName.equals("..")) {
                System.out.println(fileName);
            }
        }
        System.out.println("Enter Directory from above list: ");
        String directoryName = sc.next();
        sftpChannel.cd(path + directoryName + "/sql/");
            Vector<ChannelSftp.LsEntry> list = sftpChannel.ls(".");
        System.out.println(" Column Name----------->count");
            int count = 0;
            for (int i=0;i<colNames.size();i++) {
                count =0;
                System.out.print(colNames.get(i)+ "\t");
                for (ChannelSftp.LsEntry entry : list) {
                    String fileName = entry.getFilename();
                    if (!fileName.equals(".") && !fileName.equals("..")) {
                        //System.out.print("\t" + fileName + "\t");
                        InputStream inputStream = sftpChannel.get("/etl_uwh/prd/dic/crr_rvn/sql/" + fileName);
                        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

                        String line;
                        while ((line = reader.readLine()) != null) {
                            int index = 0;
                            while (index != -1) {
                                index = line.indexOf(colNames.get(i), index);
                                if (index != -1) {
                                    if(fileName.equals("UDM_FIN_CRR_FR_CK.sql") && colNames.get(i).equals("ORIG_SRC_SYS_CD")){
                                        System.out.println(line);
                                    }
                                    count++;
                                    index += colNames.get(i).length();
                                }
                            }
                        }
                    }

                }
                System.out.print(count);
                System.out.println();
            }
            sftpChannel.disconnect();
            channel.disconnect();
            session.disconnect();
    }
}
