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
        sb.append("Stack trace:\n");
        
        for (StackTraceElement stElement : exception.getStackTrace()) {
            String[] strackTraceMessage = stElement.toString().split("\\)");
            for (String string : strackTraceMessage) {
                sb.append("\t");
                sb.append(string);
                sb.append(")\n");
            }
        }

        return sb.toString();
    }
}
