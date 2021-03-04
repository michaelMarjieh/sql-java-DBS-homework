package techflix;

import techflix.business.Movie;
import techflix.business.MovieRating;
import techflix.business.ReturnValue;
import techflix.business.Viewer;
import techflix.data.DBConnector;
import techflix.data.PostgresSQLErrorCodes;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class Solution {

    public static void createTables() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement Movie = null;
        PreparedStatement Viewer = null;

        PreparedStatement Watched = null;
        PreparedStatement en = null;
        try {
            en = connection.prepareStatement("CREATE TYPE MovieRating AS ENUM ('LIKE', 'DISLIKE');");

            Viewer = connection.prepareStatement("CREATE TABLE Viewer\n" +
                    "(\n" +
                    "    viewer_id integer,\n" +
                    "    Name text NOT NULL ,\n" +
                    "    PRIMARY KEY (viewer_id),\n" +
                    "    CHECK (viewer_id > 0)\n" +
                    ")");
            Movie = connection.prepareStatement("CREATE TABLE Movie\n" +
                    "(\n" +
                    "    movie_id integer,\n" +
                    "    Name text NOT NULL ,\n" +
                    "    Description text NOT NULL,\n" +
                    "    PRIMARY KEY (Movie_id),\n" +
                    "    CHECK (Movie_id > 0)\n" +
                    ")");

            Watched = connection.prepareStatement("CREATE TABLE Watched\n" +
                    "(\n" +
                    "    movie_id integer, \n" +
                    "    viewer_id integer, \n" +
                    "    rating MovieRating, \n" +
                    "    PRIMARY KEY (movie_id, viewer_id), \n" +
                    "    FOREIGN KEY (movie_id) REFERENCES Movie(movie_id) ON DELETE CASCADE, \n" +
                    "    FOREIGN KEY (viewer_id) REFERENCES Viewer(viewer_id) ON DELETE CASCADE \n" +
                    ")");
            en.execute();
            Viewer.execute();
            Movie.execute();
            Watched.execute();


        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue()) {

            }
        } finally {
            try {
                en.close();
                Viewer.close();
                Movie.close();
                Watched.close();

            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
    }


    public static void clearTables() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("DELETE FROM Viewer ");
            pstmt.execute();
            pstmt = connection.prepareStatement("DELETE FROM Movie ");
            pstmt.execute();
            pstmt = connection.prepareStatement("DELETE FROM Watched ");
            pstmt.execute();


        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue()) {

            }
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
    }


    public static void dropTables() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {




            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS Viewer CASCADE");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS Movie CASCADE");
            pstmt.execute();

            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS Watched CASCADE");
            pstmt.execute();

            pstmt = connection.prepareStatement("DROP TYPE IF EXISTS MovieRating ");
            pstmt.execute();

        } catch (SQLException e) {

            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue()) {

            }
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
    }


    public static ReturnValue createViewer(Viewer viewer) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("INSERT INTO Viewer " +
                    "VALUES (?,?)");

           // System.out.printf("The string is %s   id is %d   \n" , viewer.getName(),viewer.getId());
            pstmt.setInt(1, viewer.getId());
            pstmt.setString(2,viewer.getName());


            pstmt.execute();

        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue()) {
                return ReturnValue.BAD_PARAMS;
            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {
                return ReturnValue.ALREADY_EXISTS;

            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()) {
                return ReturnValue.NOT_EXISTS;

            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue()) {
                return ReturnValue.BAD_PARAMS;
            }
            else return ReturnValue.ERROR;

        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        return ReturnValue.OK;
    }

    public static ReturnValue deleteViewer(Viewer viewer) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(
                    "DELETE FROM Viewer " +

                            "where viewer_id = ?");
            pstmt.setInt(1, viewer.getId());

            int affectedRows = pstmt.executeUpdate();
            if(affectedRows==0){
                return ReturnValue.NOT_EXISTS;
            }
            System.out.println("deleted " + affectedRows + " rows");
        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue()) {
                return ReturnValue.BAD_PARAMS;
            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {
                return ReturnValue.ALREADY_EXISTS;

            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()) {
                return ReturnValue.NOT_EXISTS;

            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue()) {
                return ReturnValue.BAD_PARAMS;
            }
            else return ReturnValue.ERROR;

        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return  ReturnValue.OK;

    }

    public static ReturnValue updateViewer(Viewer viewer) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(
                    "UPDATE Viewer " +
                            "SET Name = ? " +
                            "where viewer_id = ?");
            pstmt.setInt(2, viewer.getId());
            pstmt.setString(1, viewer.getName());
            int affectedRows = pstmt.executeUpdate();
            if(affectedRows==0){
                return ReturnValue.NOT_EXISTS;
            }
            System.out.println("changed " + affectedRows + " rows");
        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue()) {
                return ReturnValue.BAD_PARAMS;
            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {
                return ReturnValue.ALREADY_EXISTS;

            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()) {
                return ReturnValue.NOT_EXISTS;

            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue()) {
                return ReturnValue.BAD_PARAMS;
            }
            else return ReturnValue.ERROR;

        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return  ReturnValue.OK;
    }

    public static Viewer getViewer(Integer viewerId) {

        Connection connection = DBConnector.getConnection();
        Viewer n=null;
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT * FROM Viewer " +
                    "where viewer_Id = ?");
            pstmt.setInt(1, viewerId);
            ResultSet results = pstmt.executeQuery();

            if(results.next()){
                n=new Viewer();
                n.setId(results.getInt(1));
                n.setName(results.getString(2));
            }
            else{
                n=n.badViewer();
            }

            DBConnector.printResults(results);
            results.close();

        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue()) {

            }
            //  return ReturnValue.ERROR;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return n;
    }


    public static ReturnValue createMovie(Movie movie) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("INSERT INTO Movie" +
                    " VALUES (?, ?, ?)");
            pstmt.setInt(1, movie.getId());
            pstmt.setString(2, movie.getName());
            pstmt.setString(3, movie.getDescription());

            pstmt.execute();

        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue()) {
                return ReturnValue.BAD_PARAMS;
            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {
                return ReturnValue.ALREADY_EXISTS;

            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()) {
                return ReturnValue.NOT_EXISTS;

            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue()) {
                return ReturnValue.BAD_PARAMS;
            }
            else return ReturnValue.ERROR;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        return ReturnValue.OK;
    }

    public static ReturnValue deleteMovie(Movie movie) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(
                    "DELETE FROM Movie " +

                            "where movie_id = ?");
            pstmt.setInt(1, movie.getId());

            int affectedRows = pstmt.executeUpdate();
            if(affectedRows==0){
                return ReturnValue.NOT_EXISTS;
            }
            System.out.println("deleted " + affectedRows + " rows");
        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue()) {
                return ReturnValue.BAD_PARAMS;
            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {
                return ReturnValue.ALREADY_EXISTS;

            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()) {
                return ReturnValue.NOT_EXISTS;

            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue()) {
                return ReturnValue.BAD_PARAMS;
            }
            else return ReturnValue.ERROR;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return ReturnValue.OK;
    }

    public static ReturnValue updateMovie(Movie movie) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {

            pstmt = connection.prepareStatement(
                    "UPDATE Movie " +
                            "SET description = ? " +
                            "where movie_id = ?");
            pstmt.setInt(2, movie.getId());
            pstmt.setString(1, movie.getDescription());


            int affectedRows = pstmt.executeUpdate();

            if(affectedRows==0){
                return ReturnValue.NOT_EXISTS;
            }
            System.out.println("changed " + affectedRows + " rows");
        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue()) {
                return ReturnValue.BAD_PARAMS;
            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {
                return ReturnValue.ALREADY_EXISTS;

            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()) {
                return ReturnValue.NOT_EXISTS;

            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue()) {
                return ReturnValue.BAD_PARAMS;
            }
            else return ReturnValue.ERROR;

        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return ReturnValue.OK;
    }

    public static Movie getMovie(Integer movieId) {
        Connection connection = DBConnector.getConnection();
        Movie n=null;
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT * FROM Movie " +
                    "where movie_id = ?");
            pstmt.setInt(1, movieId);
            ResultSet results = pstmt.executeQuery();
            if(results.next()){
                n=new Movie();
                n.setId(results.getInt(1));
                n.setName(results.getString(2));
                n.setDescription(results.getString(3));
            }
            else{
                n=n.badMovie();
            }

            DBConnector.printResults(results);
            results.close();

        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue()) {

            }
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return n;
    }


    public static ReturnValue addView(Integer viewerId, Integer movieId) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("INSERT into Watched" +
                    " values (?,?)");

            pstmt.setInt(1, movieId);
            pstmt.setInt(2, viewerId);
            pstmt.executeUpdate();


        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue()) {
                    return ReturnValue.BAD_PARAMS;
            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {
                return ReturnValue.ALREADY_EXISTS;

            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()) {
                return ReturnValue.NOT_EXISTS;

            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue()) {
                return ReturnValue.BAD_PARAMS;
            }
            else return ReturnValue.ERROR;

        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return ReturnValue.OK;


    }

    public static ReturnValue removeView(Integer viewerId, Integer movieId) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(
                    "DELETE FROM Watched " +
                            " where viewer_id = ?" +
                            " AND movie_id = ?");
            pstmt.setInt(1, viewerId);
            pstmt.setInt(2, movieId);

            int affectedRows = pstmt.executeUpdate();
            if(affectedRows==0){
                return ReturnValue.NOT_EXISTS;
            }
            System.out.println("deleted " + affectedRows + " rows");
        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue()) {
                return ReturnValue.BAD_PARAMS;
            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {
                return ReturnValue.ALREADY_EXISTS;

            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()) {
                return ReturnValue.NOT_EXISTS;

            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue()) {
                return ReturnValue.BAD_PARAMS;
            }
            else return ReturnValue.ERROR;

        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return ReturnValue.OK;
    }

    public static Integer getMovieViewCount(Integer movieId) {
        Connection connection = DBConnector.getConnection();
        int count=0;
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT COUNT(viewer_id) FROM Watched " +
                    " where movie_id = ?");
            pstmt.setInt(1, movieId);
            ResultSet results = pstmt.executeQuery();
            results.next();
            count=results.getInt(1);
            DBConnector.printResults(results);
            results.close();

        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue()) {

            }
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        return count;
    }


    public static ReturnValue addMovieRating(Integer viewerId, Integer movieId, MovieRating rating) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {

            pstmt = connection.prepareStatement("UPDATE Watched" +
                    " Set rating=CAST(? AS MovieRating) " +
                    " where movie_id = ? AND viewer_id=?");
            pstmt.setObject(1, rating.toString());
            pstmt.setInt(2, movieId);
            pstmt.setInt(3, viewerId);
            int res=pstmt.executeUpdate();
            if(res==0){
                return ReturnValue.NOT_EXISTS;
            }


        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue()) {
                return ReturnValue.BAD_PARAMS;
            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {
                return ReturnValue.ALREADY_EXISTS;

            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()) {
                return ReturnValue.NOT_EXISTS;

            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue()) {
                return ReturnValue.BAD_PARAMS;
            }
            else return ReturnValue.ERROR;

        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        return ReturnValue.OK;
    }

    public static ReturnValue removeMovieRating(Integer viewerId, Integer movieId) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        int rs;
        try {

            pstmt = connection.prepareStatement("UPDATE watched SET rating=NULL " +
                    " where viewer_id=?  AND movie_id = ?");

            pstmt.setInt(1, viewerId);
            pstmt.setInt(2, movieId);
            rs=pstmt.executeUpdate();
            if(rs==0){
                return ReturnValue.NOT_EXISTS;
            }



        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue()) {
                return ReturnValue.BAD_PARAMS;
            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {
                return ReturnValue.ALREADY_EXISTS;

            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()) {
                return ReturnValue.NOT_EXISTS;

            }
            else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue()) {
                return ReturnValue.BAD_PARAMS;
            }
            else return ReturnValue.ERROR;

        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return ReturnValue.OK;
    }

    public static int getMovieLikesCount(int movieId) {

        Connection connection = DBConnector.getConnection();
        int count=0;
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT COUNT(viewer_id) FROM watched " +
                    " where movie_id = ? AND  rating='LIKE'");
            pstmt.setInt(1, movieId);
            ResultSet results = pstmt.executeQuery();
            results.next();
            count=results.getInt(1);
            DBConnector.printResults(results);
            results.close();

        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue()) {

            }
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        return count;
    }

    public static int getMovieDislikesCount(int movieId) {

        Connection connection = DBConnector.getConnection();
        int count=0;
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT COUNT(viewer_id) FROM watched " +
                    " where movie_id = ? AND  rating='DISLIKE'");
            pstmt.setInt(1, movieId);
            ResultSet results = pstmt.executeQuery();
            results.next();
            count=results.getInt(1);
            DBConnector.printResults(results);
            results.close();

        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue()) {

            }
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        return count;
    }

    public static ArrayList<Integer> getSimilarViewers(Integer viewerId) {
        Connection connection = DBConnector.getConnection();
        ArrayList<Integer> list=null;
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT viewer_id FROM watched \n" +
                    " where viewer_id!=? AND movie_id IN (SELECT movie_id FROM watched where viewer_id=?)\n" +
                    " Group BY viewer_id\n" +
                    " Having COUNT(viewer_id) >= 0.75*(SELECT COUNT(viewer_id) FROM watched where viewer_id=?)\n" +
                    " ORDER BY  viewer_id ASC");
            pstmt.setInt(1, viewerId);
            pstmt.setInt(2, viewerId);
            pstmt.setInt(3, viewerId);

            ResultSet results = pstmt.executeQuery();
            list=resToList(results);
            DBConnector.printResults(results);
            results.close();

        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue()) {

            }
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        return list;
    }


    public static ArrayList<Integer> mostInfluencingViewers() {

        Connection connection = DBConnector.getConnection();
        ArrayList<Integer> list=null;
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT  viewer_id FROM watched \n" +
                    " group by viewer_id\n" +
                    " order by count(*) desc,count(rating) desc, viewer_id ASC\n" +
                    " limit 10");
            ;

            ResultSet results = pstmt.executeQuery();
            list=resToList(results);
            DBConnector.printResults(results);
            results.close();

        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue()) {

            }
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        return list;
    }


    public static ArrayList<Integer> getMoviesRecommendations(Integer viewerId) {
        Connection connection = DBConnector.getConnection();
        ArrayList<Integer> list=null;
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT W.movie_id FROM " +
                    " ((select * from watched where watched.rating = 'LIKE') union\n" +
                    " (select watched.movie_id,watched.viewer_id, " +
                    " null as rating from watched where watched.rating != 'LIKE' or watched.rating is null))as W\n" +
                    " WHERE W.movie_id NOT IN (select watched.movie_id from watched where watched.viewer_id=?)\n" +
                    " AND W.viewer_id IN(\n" +
                    " SELECT viewer_id FROM watched \n" +
                    " where viewer_id!=? AND movie_id IN (SELECT movie_id FROM watched where viewer_id=?)\t\n" +
                    " Group BY viewer_id\n" +
                    " Having COUNT(viewer_id) >= 0.75*(SELECT COUNT(viewer_id) FROM watched where viewer_id=?)\n" +
                    " ORDER BY  viewer_id ASC\n" +
                    " )\n" +
                    " group by W.movie_id \n" +
                    " order by count(W.rating) desc, W.movie_id ASC \n" +
                    " limit 10");
            pstmt.setInt(1, viewerId);
            pstmt.setInt(2, viewerId);
            pstmt.setInt(3, viewerId);
            pstmt.setInt(4, viewerId);


            ResultSet results = pstmt.executeQuery();
            list=resToList(results);
            DBConnector.printResults(results);
            results.close();

        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue()) {

            }
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }


        return list;
    }


    public static ArrayList<Integer> getConditionalRecommendations(Integer viewerId, int movieId) {
        ArrayList<Integer> list=null;
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT W.movie_id FROM" +
                    " ((select * from watched where watched.rating = 'LIKE')union\n" +
                    " (select watched.movie_id,watched.viewer_id, " +
                    " null as rating from watched where watched.rating != 'LIKE' or watched.rating is null))as W\n" +
                    " WHERE W.movie_id NOT IN (select watched.movie_id from watched where watched.viewer_id=?)\n" +
                    "\tAND W.viewer_id IN(\n" +
                    "\tSELECT viewer_id FROM watched \n" +
                    "\twhere viewer_id!=? AND movie_id IN (SELECT movie_id FROM watched where viewer_id=?)\t\n" +
                    "\tGroup BY viewer_id\n" +
                    "\tHaving COUNT(viewer_id) >= 0.75*(SELECT COUNT(viewer_id) FROM watched where viewer_id=?)\n" +
                    "\tORDER BY  viewer_id ASC\n" +
                    " )\n" +
                    "\tAND W.viewer_id IN(\n" +
                    "\t\tselect viewer_id from watched where viewer_id in " +
                    " (select viewer_id where movie_id=? and viewer_id!=? and rating in " +
                    " ( select rating from watched where movie_id=?\n" +
                    "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t   and viewer_id=?))\n" +
                    "\t\n" +
                    " )\n" +
                    "group by W.movie_id \n" +
                    "order by count(W.rating) desc, W.movie_id ASC \n" +
                    "limit 10");
            pstmt.setInt(1, viewerId);
            pstmt.setInt(2, viewerId);
            pstmt.setInt(3, viewerId);
            pstmt.setInt(4, viewerId);
            pstmt.setInt(5, movieId);
            pstmt.setInt(6, viewerId);
            pstmt.setInt(7, movieId);
            pstmt.setInt(8, viewerId);
            ResultSet results = pstmt.executeQuery();
            list=resToList(results);
            DBConnector.printResults(results);
            results.close();

        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()) {

            }
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue()) {

            }
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }


        return list;

    }
    private static ArrayList<Integer> resToList(ResultSet res) throws SQLException{
        ArrayList<Integer> l= new ArrayList<>();
        while(res.next()){
            l.add(res.getInt(1));
        }
        return l;
    }
}