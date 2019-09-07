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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;

/**
 * @author Clinton Begin
 */
public class ExceptionUtil {

  private ExceptionUtil() {
    // Prevent Instantiation
  }

  /**<ol>
   * <li>
   * InvocationTargetException：<br>
   * 当java反射调用方抛出异常时，就会用InvocationTargetException将原异常包裹
   * </li>
   * <li>
   * UndeclaredThrowableException：<br>
   * java动态代理时抛出的异常。当对某接口进行动态代理，接口的方法名称上未声明某类受检异常，而方法却抛出了该异常，动态代理会将该异常用UndeclaredThrowableException包裹。
   * </li>
   *</ol>
   * <ul>
   *     综上：由于jdk动态代理内部会调用method.invoke进行调用，当method抛出异常时，则会抛出InvocationTargetException。<br>
   *    由于InvocationTargetException是受检异常，当代理接口方法未标识InvocationTargetException，则抛出UndeclaredThrowableException。<br>
   *    本方法是用于在执行反射的时候对异常进行解包装。
   * </ul>
   * @param wrapped 要被解包装的异常
   * @return 解开包装后最原始的异常
   */
  public static Throwable unwrapThrowable(Throwable wrapped) {
    Throwable unwrapped = wrapped;
    while (true) {
      if (unwrapped instanceof InvocationTargetException) {
        unwrapped = ((InvocationTargetException) unwrapped).getTargetException();
      } else if (unwrapped instanceof UndeclaredThrowableException) {
        unwrapped = ((UndeclaredThrowableException) unwrapped).getUndeclaredThrowable();
      } else {
        return unwrapped;
      }
    }
  }

}
