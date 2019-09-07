/**
 *    Copyright 2009-2019 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.reflection.property;

import java.util.Iterator;

/**
 * 属性表达式编译器
 * @author Clinton Begin
 */
public class PropertyTokenizer implements Iterator<PropertyTokenizer> {
    /**
     * 不带index（"[ ]"）的name
     */
    private String name;
    /**
     * 带有index（"[ ]"）的name
     * （只要当前层属性名称带有[]，当前层属性就会被当成是Map或者Collection或者数组类型的，参考所有调用了{@link org.apache.ibatis.reflection.wrapper.BaseWrapper#getCollectionValue(PropertyTokenizer, Object)} 的地方）
     */
    private final String indexedName;
    /**
     * index（"[ ]中的值"）
     * 属性表达式每一层都只会有一个index，像prop1[index1][index2]这样的会被认为index是"index1][index2"，参考{@link PropertyTokenizer#PropertyTokenizer(String)}64行代码
     */
    private String index;
    /**
     * 子属性（"."后面的字符串）
     */
    private final String children;

    /**
     * <p>
     *     属性表达式：prop1.prop2.prop3<br>
     * </p>
     * @param fullname 属性表达式
     */
    public PropertyTokenizer(String fullname) {
        int delim = fullname.indexOf('.');
        if (delim > -1) {
            name = fullname.substring(0, delim);
            children = fullname.substring(delim + 1);
        } else {
            name = fullname;
            children = null;
        }
        indexedName = name;
        //兼容map[index1]、list[1]等等这样的表达式，
        delim = name.indexOf('[');
        if (delim > -1) {
            //如果属性表达式中含有"["，直接认为从当前的"["开始到下一个"."之前的那个字符就是"]"，这里不做任何算法处理，直接取"["的位置到"."之前的位置前一位作为index
            index = name.substring(delim + 1, name.length() - 1);
            //取当前属性名称节点的起始值到"["之前的字符串作为name
            name = name.substring(0, delim);
        }
    }

    public String getName() {
        return name;
    }

    public String getIndex() {
        return index;
    }

    public String getIndexedName() {
        return indexedName;
    }

    public String getChildren() {
        return children;
    }

    @Override
    public boolean hasNext() {
        return children != null;
    }

    @Override
    public PropertyTokenizer next() {
        return new PropertyTokenizer(children);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Remove is not supported, as it has no meaning in the context of properties.");
    }
}
