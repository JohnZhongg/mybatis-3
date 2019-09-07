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

import org.apache.ibatis.reflection.factory.DefaultObjectFactory;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.reflection.wrapper.DefaultObjectWrapperFactory;
import org.apache.ibatis.reflection.wrapper.ObjectWrapperFactory;

/**
 * @author Clinton Begin
 */
public final class SystemMetaObject {

  /**
   * 默认的对象工厂
   */
  public static final ObjectFactory DEFAULT_OBJECT_FACTORY = new DefaultObjectFactory();
  /**
   * 默认的对象包装器工厂
   */
  public static final ObjectWrapperFactory DEFAULT_OBJECT_WRAPPER_FACTORY = new DefaultObjectWrapperFactory();
  /**
   * 一个空对象{@link NullObject}的元数据对象
   */
  public static final MetaObject NULL_META_OBJECT = MetaObject.forObject(NullObject.class, DEFAULT_OBJECT_FACTORY, DEFAULT_OBJECT_WRAPPER_FACTORY, new DefaultReflectorFactory());

  private SystemMetaObject() {
    // Prevent Instantiation of Static Class
  }

  private static class NullObject {
  }

  /**
   * 传入一个对象，使用默认的对象工厂，默认的对象包装器工厂，默认的反射信息容器工厂构造它的元数据
   * @param object 传入的对象
   * @return 传入对象的元数据对象
   */
  public static MetaObject forObject(Object object) {
    return MetaObject.forObject(object, DEFAULT_OBJECT_FACTORY, DEFAULT_OBJECT_WRAPPER_FACTORY, new DefaultReflectorFactory());
  }

}
