package com.datawarehouse.utils;

import org.apache.commons.lang3.StringUtils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


public class TimeUtils {

	public final static String sdf ="yyyy-MM-dd HH:mm:ss";
	public final static String defaultSdf ="yyyy-MM-dd HH:00:00";
	public final static String defaultDaySdf ="yyyy-MM-dd 00:00:00";
	public final static String sdf1 ="yyyy-MM-dd";
	public final static String sdf2 ="yyyyMMdd HHmmss";
	public final static String sdf3 = "yyyy-MM-dd HH:mm";
	public final static String sdf4 ="yyyy-MM-dd HH";
	public final static String hourFormat ="HH:mm";
	//时间格式
	public final static String SECONDSTR="yyyy-MM-dd HH:mm:ss";
	public final static String MINUTESTR="yyyy-MM-dd HH:mm";
	public final static String HOURSTR="yyyy-MM-dd HH";
	public final static String DAYSTR="yyyy-MM-dd";
	public final static String MONTHSTR="yyyy-MM";
	public final static String DEFAULTMINUTESTR="yyyy-MM-dd HH:mm:00";
	public final static String DEFAULTHOURSTR="yyyy-MM-dd HH:00:00";
	public final static String DEFAULTDAYSTR="yyyy-MM-dd 00:00:00";
	public final static String time="yyyyMMdd";
	public final static String SIMPLE="yyyyMMdd HHmmss";
	private static ThreadLocal<DateFormat> threadLocal_month = new ThreadLocal<DateFormat>();
	private static ThreadLocal<DateFormat> threadLocal_day = new ThreadLocal<DateFormat>();
	private static ThreadLocal<DateFormat> threadLocal_defaultday = new ThreadLocal<DateFormat>();
	private static ThreadLocal<DateFormat> threadLocal_hour = new ThreadLocal<DateFormat>();
	private static ThreadLocal<DateFormat> threadLocal_defaulthour = new ThreadLocal<DateFormat>();
	private static ThreadLocal<DateFormat> threadLocal_minute = new ThreadLocal<DateFormat>();
	private static ThreadLocal<DateFormat> threadLocal_defaultminute = new ThreadLocal<DateFormat>();
	private static ThreadLocal<DateFormat> threadLocal_second = new ThreadLocal<DateFormat>();
	private static ThreadLocal<DateFormat> threadLocal_time = new ThreadLocal<DateFormat>();

	/**
	 * 获取线程安全的DateFormat
	 * 时间格式:yyyyMMdd
	 * @return
	 */
	public static DateFormat getDateFormatByTime()
    {
        DateFormat df = threadLocal_time.get();
        if(df==null){
            df = new SimpleDateFormat(time);
            threadLocal_time.set(df);
        }
        return df;
    }

	/**
	 * 获取线程安全的DateFormat
	 * 时间格式:yyyy-MM
	 * @return
	 */
	public static DateFormat getDateFormatByMonth()
    {
        DateFormat df = threadLocal_month.get();
        if(df==null){
            df = new SimpleDateFormat(MONTHSTR);
            threadLocal_month.set(df);
        }
        return df;
    }

	/**
	 * 获取线程安全的DateFormat
	 * 时间格式:yyyy-MM-dd
	 * @return
	 */
	public static DateFormat getDateFormatByDay()
    {
        DateFormat df = threadLocal_day.get();
        if(df==null){
            df = new SimpleDateFormat(DAYSTR);
            threadLocal_day.set(df);
        }
        return df;
    }

	/**
	 * 获取线程安全的DateFormat
	 * 时间格式:yyyy-MM-dd 00:00:00
	 * @return
	 */
	public static DateFormat getDateFormatByDefaultDay()
    {
        DateFormat df = threadLocal_defaultday.get();
        if(df==null){
            df = new SimpleDateFormat(DEFAULTDAYSTR);
            threadLocal_defaultday.set(df);
        }
        return df;
    }

	/**
	 * 获取线程安全的DateFormat
	 * 时间格式:yyyy-MM-dd HH
	 * @return
	 */
	public static DateFormat getDateFormatByHour()
    {
        DateFormat df = threadLocal_hour.get();
        if(df==null){
            df = new SimpleDateFormat(HOURSTR);
            threadLocal_hour.set(df);
        }
        return df;
    }

	/**
	 * 获取线程安全的DateFormat
	 * 时间格式:yyyy-MM-dd HH:00:00
	 * @return
	 */
	public static DateFormat getDateFormatByDefaultHour()
    {
        DateFormat df = threadLocal_defaulthour.get();
        if(df==null){
            df = new SimpleDateFormat(DEFAULTHOURSTR);
            threadLocal_defaulthour.set(df);
        }
        return df;
    }

	/**
	 * 获取线程安全的DateFormat
	 * 时间格式:yyyy-MM-dd HH:mm
	 * @return
	 */
	public static DateFormat getDateFormatByMinute()
    {
        DateFormat df = threadLocal_minute.get();
        if(df==null){
            df = new SimpleDateFormat(MINUTESTR);
            threadLocal_minute.set(df);
        }
        return df;
    }

	/**
	 * 获取线程安全的DateFormat
	 * 时间格式:yyyy-MM-dd HH:mm:00
	 * @return
	 */
	public static DateFormat getDateFormatByDefaultMinute()
    {
        DateFormat df = threadLocal_defaultminute.get();
        if(df==null){
            df = new SimpleDateFormat(DEFAULTMINUTESTR);
            threadLocal_defaultminute.set(df);
        }
        return df;
    }

	/**
	 * 获取线程安全的DateFormat
	 * 时间格式:yyyy-MM-dd HH:mm:ss
	 * @return
	 */
	public static DateFormat getDateFormatBySecond()
    {
        DateFormat df = threadLocal_second.get();
        if(df==null){
            df = new SimpleDateFormat(SECONDSTR);
            threadLocal_second.set(df);
        }
        return df;
    }

	public static DateFormat getDateFormat(String str){
		if(str.equals(DAYSTR)||str.equals(sdf1)){
			return getDateFormatByDay();
		}else if(str.equals(DEFAULTDAYSTR)||str.equals(defaultDaySdf)){
			return getDateFormatByDefaultDay();
		}else if(str.equals(HOURSTR)||str.equals(sdf4)){
			return getDateFormatByHour();
		}else if(str.equals(DEFAULTHOURSTR)||str.equals(defaultSdf)){
			return getDateFormatByDefaultHour();
		}else if(str.equals(MINUTESTR)||str.equals(sdf3)){
			return getDateFormatByMinute();
		}else if(str.equals(DEFAULTMINUTESTR)){
			return getDateFormatByDefaultMinute();
		}else if(str.equals(SECONDSTR)||str.equals(sdf)){
			return getDateFormatBySecond();
		}else{
			return getDateFormatBySecond();
		}
	}

	public static Date getFormatTime(){
		Date ts = new Date(System.currentTimeMillis());
	    return ts;
	}

	public static Date getFormatTimeByStr(String time) throws ParseException{
		//sdf.parse(time);
		return getDateFormatBySecond().parse(time);
	}
	public static Date getFormatTimeByStr(String time,String sdf) throws ParseException{
		return getDateFormat(sdf).parse(time);
		//return sdf.parse(time);
	}

	public static String parseToFormatTime(Date date){
		SimpleDateFormat sdf = (SimpleDateFormat) DateFormat.getDateTimeInstance();
		return sdf.format(date);
	}
	public static String parseToFormatTime(Date date,String sdf){
		return getDateFormat(sdf).format(date);
		//return sdf.format(date);
	}



	public static Long parseTime(String time) throws Exception{
		Long result=null;
		try {
			result=getDateFormat(SECONDSTR).parse(time.toString()).getTime();
		} catch (Exception e) {
			try {
				result=getDateFormat(MINUTESTR).parse(time.toString()).getTime();
			} catch (ParseException e1) {
				result=getDateFormat(HOURSTR).parse(time.toString()).getTime();
			}
		}
		return result;
	}

	public static Date parseToDate(String time,String sdf) throws Exception{
		return getDateFormat(sdf).parse(time);
	}

	public static Long parseToLong(Date date) throws Exception{
		String timeStr=getDateFormat(TimeUtils.DEFAULTMINUTESTR).format(date);
	    Long time =parseToDate(timeStr, TimeUtils.DEFAULTMINUTESTR).getTime();
	    return time;
	}

	/**
	 * 格式化时间:XX小时xx分XX秒
	 * @param timeLen
	 * @return
	 */
	public static String parseTimeStamp(long timeLen){
		//余数超过500则进位
		long remainder=timeLen%1000;
		timeLen=timeLen/1000;
		if(remainder>=500){
			timeLen+=1;
		}
    	if(timeLen<60){
    		return timeLen+"秒";
    	}else if(timeLen>60&&timeLen<3600){
    		long minute=timeLen/60;
    	    return minute+"分"+timeLen%60+"秒";
    	}else if(timeLen>3600){
    		long hour=timeLen/(60*60);
    		long minute=(timeLen%(60*60))/60;
    	    return hour+"时"+minute+"分"+(timeLen%(60*60))%60+"秒";
    	}
    	return null;
    }

	public static Date getDateAddDays(Date date, int add_days) {
        Calendar time = Calendar.getInstance();
        time.setTime(date);
        time.add(5, add_days);
        return time.getTime();
    }
	/**
	 * 获取当天0点
	 * @return
	 */
	public static String getCurrentDay0spot(){
	      Calendar calendar = Calendar.getInstance();
          calendar.setTime(new Date());
          calendar.set(Calendar.HOUR_OF_DAY, 0);
          calendar.set(Calendar.MINUTE, 0);
          calendar.set(Calendar.SECOND, 0);
          Date zero = calendar.getTime();
          return parseToFormatTime(zero, SECONDSTR);
	}

	/**
	 * 获取当天0点
	 * @return
	 */
	public static Date getCurrentDay0spotDate(){
	      Calendar calendar = Calendar.getInstance();
          calendar.setTime(new Date());
          calendar.set(Calendar.HOUR_OF_DAY, 0);
          calendar.set(Calendar.MINUTE, 0);
          calendar.set(Calendar.SECOND, 0);
          Date zero = calendar.getTime();
          return zero;
	}

	public static Date getAddDays(String time,int add_days){
		return getDateAddDays(new Date(Long.valueOf(time)), add_days);
	}


	/**
	 * 获取星期一
	 * @param format
	 * @param len
	 * @return
	 * @throws Exception
	 */
	public static Date getMondayOfWeekTime(String format,int len) throws Exception{
		Calendar calendar1=Calendar.getInstance();
		calendar1.add(Calendar.DAY_OF_YEAR,len);
		calendar1.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
		return getFormatTimeByStr(parseToFormatTime(calendar1.getTime(),format), format);
	}

	/**
	 * 获取本月一号
	 * @param format
	 * @return
	 */
	public static Date getFirstOfMonthTime(String format) throws Exception{
		Calendar calendar1=Calendar.getInstance();
		calendar1.set(Calendar.DAY_OF_MONTH, 1);
	    return getFormatTimeByStr(parseToFormatTime(calendar1.getTime(),format), format);
	}

	/**
	 * 获取上个月一号
	 * @param format
	 * @return
	 */
	public static Date getFirstOfPreMonthTime(String format) throws Exception{
		Calendar calendar1=Calendar.getInstance();
		calendar1.add(Calendar.DAY_OF_MONTH, -calendar1.getActualMaximum(Calendar.DAY_OF_MONTH));
		calendar1.set(Calendar.DAY_OF_MONTH, 1);
	    return getFormatTimeByStr(parseToFormatTime(calendar1.getTime(),format), format);
	}

	/**
	 * 获取前len的月份
	 * @param len
	 * @return
	 */
	public static String getMonth(int len){
		Calendar c = Calendar.getInstance();
		c.setTime(new Date());
		c.add(Calendar.MONTH, -len);
		c.set(Calendar.DAY_OF_MONTH, 1);
		Date m = c.getTime();
		return getDateFormatByMonth().format(m);
	}

	public static String getFirstDayOfMonth(int len){
		Calendar c = Calendar.getInstance();
		c.setTime(new Date());
		c.add(Calendar.MONTH, -len);
		c.set(Calendar.DAY_OF_MONTH, 1);
		Date m = c.getTime();
		return getDateFormatByDefaultDay().format(m);
	}

	public static String getEndDayOfMonth(int len){
		Calendar c = Calendar.getInstance();
		c.setTime(new Date());
		c.add(Calendar.MONTH, -len);
		c.set(Calendar.DAY_OF_MONTH, 0);
		Date m = c.getTime();
		return getDateFormatByDefaultDay().format(m);
	}

	public static String parseToFormatTimeByInterval(Date time,String interval){
		if(interval!=null){
			if("d".equals(interval)){
				return getDateFormat(DAYSTR).format(time);
				//return sdf1.format(time);
			}else if("h".equals(interval)){
				return getDateFormat(MINUTESTR).format(time);
				//return sdf3.format(time);
			}else if("m".equals(interval)){
				return getDateFormat(MINUTESTR).format(time);
				//return sdf3.format(time);
			}else{
				return getDateFormat(SECONDSTR).format(time);
				//return sdf.format(time);
			}
		}else{
			return getDateFormat(SECONDSTR).format(time);
		}

	}
}
