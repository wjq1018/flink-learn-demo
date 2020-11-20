import lombok.extern.log4j.Log4j;

/**
 * @ClassName DeadLockDemo
 * @Description: //死锁
 * @Date Create in 2020/11/17
 * @Author wjq
 */
@Log4j
public class DeadLockDemo {
    private static String A = "A";
    private static String B = "B";

    public static void main(String[] args) {
        new DeadLockDemo().deadLock();
        System.out.println("执行完成~~~");
    }


    private void deadLock() {
        Thread t1 = new Thread(new Runnable(){
            @Override
            public void run() {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                synchronized (A) {
                    System.out.println("t1 --A");
                }
                synchronized (B){
                    System.out.println("t1 --B");
                }
            }
        });

        Thread t2 = new Thread(new Runnable(){
            @Override
            public void run() {
                synchronized (B){
                    synchronized (A){
                        System.out.println("t2 ==B--A");
                    }
                }
            }
        });
        t1.start();
        t2.start();

    }
}
