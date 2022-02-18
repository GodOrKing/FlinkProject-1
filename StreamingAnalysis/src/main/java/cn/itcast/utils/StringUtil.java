package cn.itcast.utils;

/**
 * 字符串处理的工具类
 * 1）字符串倒序方法
 */
public class StringUtil {

    /**
     * 字符串倒序，实现方式有：递归（不推荐）、数组倒序拼接、冒泡对调、使用StringBuffer的reverse等实现
     * 冒泡对调（推荐）
     * @param orig
     * @return
     */
    public static String reverse(String orig){
        char[] chars = orig.toCharArray();
        int n = chars.length - 1;
        int halfLength = n / 2;
        for (int i = 0; i <= halfLength ; i++) {
            char temp = chars[i];
            chars[i] = chars[n-i];
            chars[n-i] = temp;
        }
        return new String(chars);
    }

    public static void main(String[] args) {
        System.out.println(reverse("hello"));
    }
}
