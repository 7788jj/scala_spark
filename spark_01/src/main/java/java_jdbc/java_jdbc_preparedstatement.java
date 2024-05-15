package java_jdbc;

import com.mysql.cj.jdbc.Driver;
/*
todo  statement可能发生sql注入的危险所以使用这个来动态加载
TODO 查询语句
 */
import java.sql.*;

/*
java的jdbc固定链接步骤
TODO 1. 注册驱动【依赖的jar包 进行安装】
TODO 2. 获取连接【connection建立连接】
TODO 3. 创建发送sql语句对象【statement 创建发送sql语句的statement】
TODO 4. 发送sql语句，并获取返回结果【statement发送sql语句到数据库 并且取得返回结构】
TODO 5. 结果集解析【将result结果解析出来】
TODO 6. 资源关闭【释放resultset、statement、connection】
 */
public class java_jdbc_preparedstatement {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        //todo  1.通过反射来注册驱动
        Class.forName("com.mysql.cj.jdbc.Driver");

        //todo 2.建立连接
        Connection connection = DriverManager.getConnection("jdbc:mysql://master:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode = true", "root", "123456");

        //todo 3.写动态sql语句，使用？来代替动态部分
        String sql = "select id ,account ,password, nickname from atguigu.t_user ";

        //todo 4.创建preparestatement对象来传入sql
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        //todo 5.发送sql
        ResultSet resultSet = preparedStatement.executeQuery(sql);

        //TODO 6.结果集的解析
        while (resultSet.next()){
            int id = resultSet.getInt("id");
            String account = resultSet.getString("account");
            String password = resultSet.getString("password");
            String nickname = resultSet.getString("nickname");
            System.out.println("id="+ id +"account = "+account + "password =" + password +"nickname =" + nickname);
        }


        //todo 7.关闭资源
        resultSet.close();
        preparedStatement.close();
        connection.close();


    }
}
