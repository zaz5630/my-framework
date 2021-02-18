package cn.azzhu.flink.stream.exception;

/**
 * 自定义异常类
 * @author azzhu
 * @since 2021-02-18 23:06
 */
public class TimeCharacteristicException extends RuntimeException {

    private static final long serialVersionUID = -6203713473183995313L;

    public TimeCharacteristicException() {
        super();
    }

    public TimeCharacteristicException(String message) {
        super(message);
    }

    public TimeCharacteristicException(String message, Throwable cause) {
        super(message, cause);
    }

    public TimeCharacteristicException(Throwable cause) {
        super(cause);
    }

    protected TimeCharacteristicException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
