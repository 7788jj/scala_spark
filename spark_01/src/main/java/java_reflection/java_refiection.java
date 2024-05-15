package java_reflection;
/*
todo  反射会加载类的构造器，成员变量，成员方法.静态代码块，反射就是获取类加载的class对象
 */

public class java_refiection {
    public static void main(String[] args) throws ClassNotFoundException {
        //发射获取class变量与三种方法
        //todo  1
        Class<student> c1 = student.class;
        //TODO 2.调用class的forname方法来获取类对象
        Class.forName("java_reflection.student");
        //todo  3.new student 对象来getclass
        student student = new student();
        Class  c2= student.getClass();

    }
}
