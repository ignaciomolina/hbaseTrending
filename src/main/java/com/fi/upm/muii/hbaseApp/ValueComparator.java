package com.fi.upm.muii.hbaseApp;

import java.text.Collator;
import java.util.Comparator;
import java.util.Map;

/**
 * Created by SoraK on 21/12/2015.
 */
public class ValueComparator implements Comparator<String> {

    Map<String, Integer> base;

    public ValueComparator(Map<String, Integer> base) {
        this.base = base;
    }

    @Override
    public int compare(String a, String b) {
    	Collator c = Collator.getInstance();
        if (base.get(a) < base.get(b)) {
            return 1;
        } else if (base.get(a)==base.get(b)) {
        	return c.compare(a, b);
        }
        else
            return -1;
    }
}
