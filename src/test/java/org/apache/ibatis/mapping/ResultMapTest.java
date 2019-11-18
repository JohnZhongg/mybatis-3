package org.apache.ibatis.mapping;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.reflection.ParamNameUtil;
import org.junit.Test;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: honphan.john
 * @date: 2019-09-24 00:10
 * @description:
 */
public class ResultMapTest {

    @Test
    public void testGetArgNames() throws NoSuchMethodException {
        Constructor<A> declaredConstructor = A.class.getDeclaredConstructor(String.class, String.class,String.class);
        List<String> argNames = A.getArgNames(declaredConstructor);
        System.out.println();
    }


}

class A{
    A(String a, String b, String c) {

    }

    public static List<String> getArgNames(Constructor<?> constructor) {
        List<String> paramNames = new ArrayList<>();
        List<String> actualParamNames = null;
        final Annotation[][] paramAnnotations = constructor.getParameterAnnotations();
        int paramCount = paramAnnotations.length;
        for (int paramIndex = 0; paramIndex < paramCount; paramIndex++) {
            String name = null;
            for (Annotation annotation : paramAnnotations[paramIndex]) {
                if (annotation instanceof Param) {
                    name = ((Param) annotation).value();
                    break;
            }
            }
            if (name == null && true) {
                if (actualParamNames == null) {
                    actualParamNames = ParamNameUtil.getParamNames(constructor);
                }
                if (actualParamNames.size() > paramIndex) {
                    name = actualParamNames.get(paramIndex);
                }
            }
            paramNames.add(name != null ? name : "arg" + paramIndex);
        }
        return paramNames;
    }
}