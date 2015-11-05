import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;


public class TimeTest {

	public static void main(String[] args) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
		Calendar cal = Calendar.getInstance();
		System.out.println(dateFormat.format(cal.getTime())); //2014/08/06 16:00:22
		System.out.println("Hello!");
	}
}
