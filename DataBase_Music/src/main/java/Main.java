import java.sql.*;

public class Main {

    private static final String PROTOCOL = "jdbc:postgresql://";        // URL-prefix
    private static final String DRIVER = "org.postgresql.Driver";       // Driver name
    private static final String URL_LOCALE_NAME = "localhost/";         // ваш компьютер + порт по умолчанию

    private static final String DATABASE_NAME = "Music";          // FIXME имя базы

    public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;
    public static final String USER_NAME = "postgres";                  // FIXME имя пользователя
    public static final String DATABASE_PASS = "postgres";

    public static void main(String[] args) {

        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");

        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {

            getArtists(connection); System.out.println();
            getAlbum(connection); System.out.println();
            getSongs(connection); System.out.println();
            getAlbumByArtist(connection, "The Weekend");
            getSongByAlbum(connection, "Starboy");
            getSongLess(connection, Time.valueOf("00:03:00"));
            getAlbumEarly(connection, Date.valueOf("2005-10-24"));
            getSongByMAXRating(connection);
            getArtistByAge(connection, 35);
            getAlbumWithBestSong(connection);
            getSongByRatingAndBetween(connection, 7.3, Time.valueOf("00:02:00"), Time.valueOf("00:02:40"));
            addArtist(connection, "МакSим", 41);
            addArtist(connection, "Нервы");
            addAlbum(connection, "МакSим", "МОЙ РАЙ", Date.valueOf("2007-11-15"));
            updateArtist(connection, "Lana Del Rey", 39);
            removeArtist(connection, "T-Fest");
            addAlbum(connection, "T-Fest", "0372", Date.valueOf("2013-10-21"));

        } catch (SQLException e) {
            if (e.getSQLState().startsWith("23")){
                System.out.println("Произошло дублирование данных");
            } else throw new RuntimeException(e);
        }
    }

    public static void checkDriver () {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту согласно инструкции.");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB () {
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        } catch (SQLException e) {
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе или разверните локально резервную копию согласно инструкции");
            throw new RuntimeException(e);
        }
    }

    private static void getArtists(Connection connection) throws SQLException{  // вывод всех артистов
        String columnName0 = "id", columnName1 = "name", columnName2 = "age";
        int param0;
        String param1;
        int param2;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM artists;");

        while (rs.next()) {
            param2 = rs.getInt(columnName2);
            param1 = rs.getString(columnName1);
            param0 = rs.getInt(columnName0);
            System.out.println(param0 + " | " + param1 + " | " + param2);
        }

        statement.close();
    }
    static void getAlbum(Connection connection) throws SQLException {   // вывод всех альбомов
        String columnName0 = "id", columName1 = "id_artist", columName2 = "name", columName3 = "release";
        String param2;
        int param0, param1;
        Date param3;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM albums;");

        while (rs.next()){
            param3 = rs.getDate(columName3);
            param2 = rs.getString(columName2);
            param1= rs.getInt(columName1);
            param0 = rs.getInt(columnName0);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3);
        }

        statement.close();
    }

    static void getSongs(Connection connection) throws SQLException{    // вывод всех песен
        String columName0 = "id", columName1 = "id_album", columName2 = "name", columName3 = "length", columName4 = "rating";
        int param0, param1;
        String param2;
        Time param3;
        double param4;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM songs;");

        while (rs.next()){
            param4 = rs.getDouble(columName4);
            param3 = rs.getTime(columName3);
            param2 = rs.getString(columName2);
            param1 = rs.getInt(columName1);
            param0 = rs.getInt(columName0);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4);
        }

        statement.close();
    }

    static void getAlbumByArtist(Connection connection, String name_artist) throws SQLException {      // Вывод всех альбомов конкретного артиста

        String columName0 = "name_artist", columName1 = "name_album";
        String param0, param1;

        PreparedStatement preparedStatement = connection.prepareStatement(
                "SELECT artists.name AS name_artist,\n" +
                "albums.name AS name_album\n" +
                "FROM artists\n" +
                "JOIN albums\n" +
                "ON artists.id = albums.id_artist\n" +
                "WHERE artists.name = ?;");
        preparedStatement.setString(1, name_artist);
        ResultSet rs = preparedStatement.executeQuery();

        while (rs.next()){
            param1 = rs.getString(columName1);
            param0 = rs.getString(columName0);
            System.out.println(param0 + " | " + param1);
        }

        preparedStatement.close();
    }

    static  void getSongByAlbum(Connection connection, String name_album) throws SQLException {   // Вывод всех треков из одного альбома

        String columName0 = "name_album", columName1 = "name_song";
        String param0, param1;

        PreparedStatement preparedStatement = connection.prepareStatement(
                "SELECT albums.name AS name_album,\n" +
                "songs.name AS name_song\n" +
                "FROM albums\n" +
                "JOIN songs\n" +
                "ON albums.id = songs.id_album\n" +
                "WHERE albums.name = ?;");
        preparedStatement.setString(1, name_album);
        ResultSet rs = preparedStatement.executeQuery();

        while (rs.next()){
            param1 = rs.getString(columName1);
            param0 = rs.getString(columName0);
            System.out.println(param0 + " | " + param1);
        }

        preparedStatement.close();
    }

    static void getSongLess(Connection connection, Time time) throws SQLException{  //  Вывод всех треков короче 3 минут

        String columName0 = "name_song", columName1 = "length";
        String param0;
        Time param1;

        PreparedStatement preparedStatement = connection.prepareStatement(
                "SELECT name AS name_song,\n" +
                "length\n" +
                "FROM songs\n" +
                "WHERE length < ?;");
        preparedStatement.setTime(1, time);
        ResultSet rs = preparedStatement.executeQuery();

        while (rs.next()){
            param1 = rs.getTime(columName1);
            param0 = rs.getString(columName0);
            System.out.println(param0 + " | " + param1);
        }

        preparedStatement.close();
    }

    static void getAlbumEarly(Connection connection, Date date) throws SQLException {   // Вывод всех альбомов, выпущенных раньше определенной даты

        String columName0 = "name_album", columName1 = "release";
        String param0;
        Date param1;

        PreparedStatement preparedStatement = connection.prepareStatement(
                "SELECT name AS name_album,\n" +
                "release\n" +
                "FROM albums\n" +
                "WHERE release < ?;");
        preparedStatement.setDate(1, date);
        ResultSet rs = preparedStatement.executeQuery();

        while (rs.next()){
            param1 = rs.getDate(columName1);
            param0 = rs.getString(columName0);
            System.out.println(param0 + " | " + param1);
        }

        preparedStatement.close();
    }

    static void getSongByMAXRating(Connection connection) throws SQLException {   //Вывод первых 10 треков с наивысшим рейтингом

        String columName0 = "name_song", columName1 = "rating";
        String param0;
        Double param1;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(
                "SELECT name AS name_song,\n" +
                        "rating\n" +
                        "FROM songs\n" +
                        "ORDER BY rating DESC\n" +
                        "LIMIT 10;");

        while (rs.next()){
            param1 = rs.getDouble(columName1);
            param0 = rs.getString(columName0);
            System.out.println(param0 + " | " + param1);
        }

        statement.close();
    }

    static void getArtistByAge(Connection connection, int age) throws SQLException {   //Вывод всех исполнителей моложе 30

        String columName0 = "name_artist", columName1 = "age";
        String param0;
        int param1;

        PreparedStatement preparedStatement = connection.prepareStatement(
                "SELECT name AS name_artist,\n" +
                "age\n" +
                "FROM artists\n" +
                "WHERE age < ?;");
        preparedStatement.setInt(1, age);
        ResultSet rs = preparedStatement.executeQuery();

        while (rs.next()){
            param1 = rs.getInt(columName1);
            param0 = rs.getString(columName0);
            System.out.println(param0 + " | " + param1);
        }

        preparedStatement.close();
    }

    static  void getAlbumWithBestSong(Connection connection) throws SQLException{    //Вывод альбома, которому принадлежит самая рейтинговая песня
        String columName0 = "name_album", columName1 = "name_song", columName2 = "rating";
        String param0, param1;
        Double param2;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(
                "SELECT albums.name AS name_album,\n" +
                "songs.name AS name_song,\n" +
                "songs.rating\n" +
                "FROM songs\n" +
                "JOIN albums\n" +
                "ON songs.id_album = albums.id\n" +
                "ORDER BY songs.rating DESC\n" +
                "LIMIT 1;");

        while (rs.next()){
            param2 = rs.getDouble(columName2);
            param1 = rs.getString(columName1);
            param0 = rs.getString(columName0);
            System.out.println(param0 + " | " + param1 + " | " + param2);
        }

        statement.close();
    }

    static void getSongByRatingAndBetween(Connection connection, Double rating, Time time1, Time time2) throws SQLException {  //Вывод всех треков, длительность которых  в диапазоне от 2 до 2.40 минут и рейтингом не ниже 7.3
        String columName0 = "name_song", columName1 = "length", columName2 = "rating";
        String param0;
        Time param1;
        Double param2;

        PreparedStatement preparedStatement = connection.prepareStatement(
                "SELECT name AS name_song,\n" +
                        "length,\n" +
                        "rating\n" +
                        "FROM songs\n" +
                        "WHERE length BETWEEN ? AND ? AND rating > ?;");
        preparedStatement.setTime(1, time1);
        preparedStatement.setTime(2, time2);
        preparedStatement.setDouble(3, rating);
        ResultSet rs = preparedStatement.executeQuery();

        while (rs.next()){
            param2 = rs.getDouble(columName2);
            param1 = rs.getTime(columName1);
            param0 = rs.getString(columName0);
            System.out.println(param0 + " | " + param1 + " | " + param2);
        }

        preparedStatement.close();
    }

    static void addArtist(Connection connection, String name, int age) throws SQLException {  // добавить артиста
        if (name == null || name.isBlank() || age < 0) return;

        PreparedStatement preparedStatement = connection.prepareStatement(
                "INSERT INTO artists (name, age) VALUES (?, ?) returning id ;", Statement.RETURN_GENERATED_KEYS);
        preparedStatement.setString(1, name);
        preparedStatement.setInt(2, age);

        int count = preparedStatement.executeUpdate();
        ResultSet rs = preparedStatement.getGeneratedKeys();

        while (rs.next()){
            System.out.println("Идентификатор артиста " + rs.getInt(1));
        }

        System.out.println("INSERTed " + count + " артист");
        getArtists(connection);
        preparedStatement.close();
    }

    static void addArtist(Connection connection, String name) throws SQLException {   // добавить артиста без возрата (группа)
        if (name == null || name.isBlank() ) return;

        PreparedStatement preparedStatement = connection.prepareStatement(
                "INSERT INTO artists (name) VALUES (?) returning id ;", Statement.RETURN_GENERATED_KEYS);
        preparedStatement.setString(1, name);

        int count = preparedStatement.executeUpdate();
        ResultSet rs = preparedStatement.getGeneratedKeys();

        while (rs.next()){
            System.out.println("Идентификатор артиста " + rs.getInt(1));
        }

        System.out.println("INSERTed " + count + " артист");
        getArtists(connection);
        preparedStatement.close();
    }

    static void addAlbum(Connection connection, String name_artist, String name_album, Date release) throws SQLException {   // добавить альбом
        if (name_artist == null || name_artist.isBlank() || name_album == null || name_album.isBlank()) return;

        int param0 = -1;

        PreparedStatement preparedStatement0 = connection.prepareStatement(
                "SELECT id\n" +
                        "FROM artists\n" +
                        "WHERE name = ?;");
        preparedStatement0.setString(1, name_artist);
        ResultSet rs0 = preparedStatement0.executeQuery();

        while (rs0.next()){
            param0 = rs0.getInt(1);
            System.out.println("id артиста " + param0);
        }

        preparedStatement0.close();

        if (param0 != -1){
            PreparedStatement preparedStatement1 = connection.prepareStatement(
                    "INSERT INTO albums(id_artist, name, release) VALUES\n" +
                            "(?, ?, ?)\n" +
                            "RETURNING id;");
            preparedStatement1.setInt(1, param0);
            preparedStatement1.setString(2, name_album);
            preparedStatement1.setDate(3, release);

            ResultSet rs1 = preparedStatement1.executeQuery();

            while (rs1.next()){
                System.out.println("Идентификатор альбома " + rs1.getInt(1));
            }

            System.out.println("INSERTed 1 альбом");
            preparedStatement1.close();
        }
        else System.out.println("Такого артиста нет");
    }

    static void updateArtist(Connection connection, String name_artist, int new_age) throws SQLException {   // изменить возраст артиста
        if (name_artist == null || name_artist.isBlank() || new_age < 0) return;

        int param0, param2;
        String param1;

        PreparedStatement preparedStatement = connection.prepareStatement(
                "UPDATE artists \n" +
                        "SET age = ?\n" +
                        "WHERE name = ?\n" +
                        "RETURNING id, name AS name_artist,\n" +
                        "age;");
        preparedStatement.setInt(1, new_age);
        preparedStatement.setString(2, name_artist);
        ResultSet rs = preparedStatement.executeQuery();

        while (rs.next()){
            param2 = rs.getInt(3);
            param1 = rs.getString(2);
            param0 = rs.getInt(1);
            System.out.println("UPDATEd 1 артист");
            System.out.println(param0 + " | " + param1 + " | " + param2);
        }

        preparedStatement.close();
    }

    static void removeArtist(Connection connection, String name_artist) throws SQLException {  // удалить артиста
        if (name_artist == null || name_artist.isBlank()) return;

        PreparedStatement preparedStatement = connection.prepareStatement(
                "DELETE from artists\n" +
                        " WHERE name=?;");
        preparedStatement.setString(1, name_artist);
        preparedStatement.executeUpdate();

        System.out.println("DELETEd 1 артист: " + name_artist);
        preparedStatement.close();
    }
}

