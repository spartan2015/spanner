package com.excellenceengineeringsolutions.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

public class Jdbc3Update
{

	private final static Logger LOG = Logger.getLogger(Jdbc3Update.class.getSimpleName());

	public static void main(String[] ar) {

		Connection connection = null;
		try {

			connection = DriverManager.getConnection("jdbc:oracle:thin:@//10.231.196.185:1533/CUSTDB", "adv", "2Ab}7C");

			//connection.createStatement().execute("insert into book(id,name) values(1,'Test')");

			PreparedStatement statement = connection.prepareStatement("update book set name=? where id = ?");
			statement.setString(1, "3Mastering JDBC 3rd Edition");
			statement.setLong(2, 1);

			statement.execute();

			System.out.println("update count:" + statement.getUpdateCount());

			ResultSet rs = connection.createStatement().executeQuery("select * from book");

			rs.getInt(1);
			if (rs.next())
			{
				System.out.println(
					rs.getString(2)
				);
			}

		} catch (SQLException ex) {
			if (connection != null) {
				try {
					connection.rollback();
				} catch (SQLException e) {
					LOG.severe("failed to rollback the transaction: " + e);
				}
			}
			LOG.severe("SQLException" + ex);
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
					LOG.severe("failed to close connection: " + e);
				}
			}
		}
	}

	public void resultSetUpdate() {

		Connection connection = null;
		try {
			String user = "root";
			String password = "1234";
			String url = "jdbc:mysql://localhost:3306/test";
			connection = DriverManager.getConnection(url, user, password);
			connection.setAutoCommit(false);

			Statement statement = connection.createStatement(ResultSet.CONCUR_UPDATABLE, ResultSet.CONCUR_UPDATABLE);

			ResultSet resultSet = statement.executeQuery("select * from books");

			while (resultSet.next()) {
				if (resultSet.getLong(1) == 1) {

					resultSet.updateString("title", "Mastering JDBC 3rd Edition");

					resultSet.updateRow();
				}
			}

			connection.commit();

		} catch (SQLException ex) {
			if (connection != null) {
				try {
					connection.rollback();
				} catch (SQLException e) {
					LOG.severe("failed to rollback the transaction: " + e);
				}
			}
			LOG.severe("SQLException" + ex);
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
					LOG.severe("failed to close connection: " + e);
				}
			}
		}
	}

}
