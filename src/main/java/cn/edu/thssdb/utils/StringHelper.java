package cn.edu.thssdb.utils;

public class StringHelper {
    static public int indexOf(String[] array, String target) {
        if (array == null || array.length == 0)
            return -1;
        int length = array.length;
        for (int i = 0; i < length; i++) {
            if (array[i].equals(target)) {
                return i;
            }
        }
        return -1;
    }
}
