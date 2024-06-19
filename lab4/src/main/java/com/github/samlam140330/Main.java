package com.github.samlam140330;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Main {
    public static void main(String[] args) {
        Credential credential = new Credential();
        try {
            String connectStr = String.format("jdbc:mysql://%s:%d", credential.endpoint, credential.port);
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection con = DriverManager.getConnection(connectStr, credential.username, credential.password);
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("show processlist");
            System.out.println("DB: " + credential.endpoint);
            System.out.println("Connections:");
            while (rs.next()) {
                System.out.println(rs.getString(3));
            }
            con.close();
        } catch (Exception exception) {
            System.out.println("Error: " + exception.getMessage());
        }
    }
}
