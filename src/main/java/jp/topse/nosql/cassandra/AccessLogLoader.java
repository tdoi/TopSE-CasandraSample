package jp.topse.nosql.cassandra;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AccessLogLoader {
    public List<Access> load(String path) {
        List<Access> list = new LinkedList<Access>();

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(path))));
            Pattern pattern = Pattern.compile("^(.*?)(\\?(.*?))?,(.*?)$");
            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                Access access = parseAccess(pattern, line);
                if (access != null) {
                    list.add(access);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return list;
    }
    
    Access parseAccess(Pattern pattern, String line) {
        Matcher matcher = pattern.matcher(line);
        if (!matcher.matches()) {
            System.err.println("Invalid Format Line: " + line);
            return null;
        }

        String target = matcher.group(1);
        String paramStr = matcher.group(2);
        String referer = matcher.group(4);
        Map<String, String> params = new HashMap<String, String>();
        if (paramStr != null) {
            for (String pair : paramStr.split("&")) {
                String[] items = pair.split("=", 2);
                params.put(items[0], items[1]);
            }
        }
        
        return new Access(target, params, referer);
    }
}
