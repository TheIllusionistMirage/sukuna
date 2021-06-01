package app.sukuna.sukunaengine.utils;

public class ErrorHandlingUtils {
    public static String getFormattedExceptionDetails(String errorMessage, Exception exception) {
        StringBuilder sb = new StringBuilder();

        sb.append("Error message: ");
        sb.append(errorMessage);
        sb.append("\n");
        sb.append("Exception(s) occurred: ");
        sb.append(exception.getMessage());
        sb.append("\n");
        sb.append("Stack trace: ");
        sb.append(exception.getStackTrace().toString());

        return sb.toString();
    }
}
