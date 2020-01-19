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
package org.apache.ibatis.parsing;

/**
 * Token 处理器接口
 *
 * @author Clinton Begin
 */
public interface TokenHandler {

    /**
     * 处理 Token (本接口方法只是定义了如何处理Token中的内容，如何识别Token的逻辑并不在本方法内，应该是先进行了识别Token的逻辑并取出Token之后再调用本方法进行Token的处理，如何识别Token参考{@link GenericTokenParser#parse(String)})
     *
     * @param content Token 字符串
     * @return 处理后的结果
     */
    String handleToken(String content);

}