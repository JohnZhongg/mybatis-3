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
package org.apache.ibatis.reflection;

/**
 * @author JohnZhong
 *
 */
public interface ReflectorFactory {

  /**
   *是否开启了缓存反射信息容器
   * @return
   */
  boolean isClassCacheEnabled();

  /**
   * 缓存反射信息容器的开关
   * @param classCacheEnabled
   */
  void setClassCacheEnabled(boolean classCacheEnabled);

  /**
   * 根据传入的Class获取其反射信息封装到Reflector容器并返回，如果开启了缓存Reflector容器，就会
   * 从缓存Reflector的map中获取，如果获取不到再去读取.class文件进行获取信息封装Reflector然后
   * 放入到Reflector缓存map中再返回
   * @param type 要获取反射信息的Type
   * @return
   */
  Reflector findForClass(Class<?> type);
}