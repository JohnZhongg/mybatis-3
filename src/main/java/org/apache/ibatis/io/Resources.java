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
package org.apache.ibatis.io;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.util.Properties;

/**
 * A class to simplify access to resources through the classloader.<br>
 * 本类用于简易地通过指定地类加载器获取资源。
 *
 * @author Clinton Begin
 */
public class Resources {

  /**
   * 类加载器包装器
   */
  private static ClassLoaderWrapper classLoaderWrapper = new ClassLoaderWrapper();

  /**
   * 指定的字符集，在将加载的字节流转换成字符流的时候使用，通过本类中相应的getter和setter来进行取值和赋值
   */
  private static Charset charset;

  Resources() {
  }

  /**
   * 返回默认的类加载器{@link ClassLoaderWrapper#defaultClassLoader}（默认是NULL）
   * @return 默认类加载器
   */
  public static ClassLoader getDefaultClassLoader() {
    return classLoaderWrapper.defaultClassLoader;
  }

  /**
   * 设置默认类加载器{@link ClassLoaderWrapper#defaultClassLoader}（默认是NULL）
   *
   * @param defaultClassLoader 要设置的默认类加载器的值
   */
  public static void setDefaultClassLoader(ClassLoader defaultClassLoader) {
    classLoaderWrapper.defaultClassLoader = defaultClassLoader;
  }

  /**
   *  直接调用{@link Resources#getResourceURL(ClassLoader, String)}，没有指定自定义的类加载，
   *  最优先使用默认的类加载器来加载资源，如果没有使用{@link Resources#setDefaultClassLoader(ClassLoader)}来设置默认类加载器，那么
   *  默认的类加载器就是NULL，那么最优先使用的类加载器就是当前线程的类加载器
   *
   * @param resource 要加载的而自愿
   * @return 加载的资源
   * @throws IOException 如果加载不到资源就抛出异常
   */
  public static URL getResourceURL(String resource) throws IOException {
      // issue #625
      return getResourceURL(null, resource);
  }

  /**
   * 直接调用{@link ClassLoaderWrapper#getResourceAsURL(String, ClassLoader)}加载资源
   *
   * @param loader 指定的自定义类加载器，即最优先用来加载资源的类加载器
   * @param resource 要加载的资源
   * @return 加载的资源
   * @throws IOException 如果资源找不到就抛出异常
   */
  public static URL getResourceURL(ClassLoader loader, String resource) throws IOException {
    URL url = classLoaderWrapper.getResourceAsURL(resource, loader);
    if (url == null) {
      throw new IOException("Could not find resource " + resource);
    }
    return url;
  }

  /**
   *  直接调用{@link Resources#getResourceAsStream(ClassLoader, String)}，没有指定自定义的类加载，
   *  最优先使用默认的类加载器来加载资源，如果没有使用{@link Resources#setDefaultClassLoader(ClassLoader)}来设置默认类加载器，那么
   *  默认的类加载器就是NULL，那么最优先使用的类加载器就是当前线程的类加载器
   *
   * @param resource 要加载的资源
   * @return 加载的资源
   * @throws IOException If the resource cannot be found or read
   */
  public static InputStream getResourceAsStream(String resource) throws IOException {
    return getResourceAsStream(null, resource);
  }

  /**
   * 直接调用{@link ClassLoaderWrapper#getResourceAsURL(String, ClassLoader)}加载资源
   *
   * @param loader   指定的自定义类加载器，即最优先用来加载资源的类加载器
   * @param resource The resource to find
   * @return The resource
   * @throws IOException If the resource cannot be found or read
   */
  public static InputStream getResourceAsStream(ClassLoader loader, String resource) throws IOException {
    InputStream in = classLoaderWrapper.getResourceAsStream(resource, loader);
    if (in == null) {
      throw new IOException("Could not find resource " + resource);
    }
    return in;
  }

  /**
   * 调用{@link Resources#getResourceAsStream(String)}来加载资源，然后使用{@link Properties#load(InputStream)}将资源加载到内存为一个Properties对象
   *
   * @param resource 要加载的资源
   * @return 加载的资源的Properties形式
   * @throws IOException 如果加载不到资源就抛出异常
   */
  public static Properties getResourceAsProperties(String resource) throws IOException {
    Properties props = new Properties();
    try (InputStream in = getResourceAsStream(resource)) {
      props.load(in);
    }
    return props;
  }

  /**
   * 调用{@link Resources#getResourceAsStream(ClassLoader, String)}来加载资源，然后使用{@link Properties#load(InputStream)}将资源加载到内存为一个Properties对象
   *
   * @param loader 指定的自定义类加载器，即最优先用来加载资源的类加载器
   * @param resource The resource to find
   * @return The resource
   * @throws IOException If the resource cannot be found or read
   */
  public static Properties getResourceAsProperties(ClassLoader loader, String resource) throws IOException {
    Properties props = new Properties();
    try (InputStream in = getResourceAsStream(loader, resource)) {
      props.load(in);
    }
    return props;
  }

  /**
   * 调用{@link Resources#getResourceAsStream(String)}加载资源为字节流，然后使用{@link InputStreamReader}将其转换成字符流，
   * 如果{@link Resources#charset}不为NULL，就使用该字符集来作为转换流的编码进行流转换
   *
   * @param resource 要加载的资源
   * @return 经加载并转换之后的字符流
   * @throws IOException If the resource cannot be found or read
   */
  public static Reader getResourceAsReader(String resource) throws IOException {
    Reader reader;
    if (charset == null) {
      reader = new InputStreamReader(getResourceAsStream(resource));
    } else {
      reader = new InputStreamReader(getResourceAsStream(resource), charset);
    }
    return reader;
  }

  /**
   * 调用{@link Resources#getResourceAsStream(ClassLoader, String)}加载资源为字节流，然后使用{@link InputStreamReader}将其转换成字符流，
   * 如果{@link Resources#charset}不为NULL，就使用该字符集来作为转换流的编码进行流转换
   *
   * @param loader 指定的自定义类加载器，最优先被用来加载资源
   * @param resource 要加载的资源
   * @return 经加载并转换之后的字符流
   * @throws IOException If the resource cannot be found or read
   */
  public static Reader getResourceAsReader(ClassLoader loader, String resource) throws IOException {
    Reader reader;
    if (charset == null) {
      reader = new InputStreamReader(getResourceAsStream(loader, resource));
    } else {
      reader = new InputStreamReader(getResourceAsStream(loader, resource), charset);
    }
    return reader;
  }

  /**
   * 调用{@link Resources#getResourceURL(String)}获取一个相关资源的URL形式，<br>
   * 然后调用{@link URL#getFile()}获取资源的文件形式返回
   *
   * @param resource 要加载的资源
   * @return 加载的资源的File形式
   * @throws IOException If the resource cannot be found or read
   */
  public static File getResourceAsFile(String resource) throws IOException {
    return new File(getResourceURL(resource).getFile());
  }

  /**
   * 调用{@link Resources#getResourceURL(ClassLoader, String)}获取一个相关资源的URL形式，<br>
   * 然后调用{@link URL#getFile()}获取资源的文件形式返回，本方法可指定被优先用来加载资源的类加载器
   *
   * @param loader 指定的自定义类加载器，被最优先用来加载资源
   * @param resource 要加载的资源
   * @return 加载的资源的File形式
   * @throws IOException If the resource cannot be found or read
   */
  public static File getResourceAsFile(ClassLoader loader, String resource) throws IOException {
    return new File(getResourceURL(loader, resource).getFile());
  }

  /**
   * 将一个远程URL链接加载到字节输入流
   *
   * @param urlString 要加载的字节输入流的URL
   * @return An input stream with the data from the URL
   * @throws IOException If the resource cannot be found or read
   */
  public static InputStream getUrlAsStream(String urlString) throws IOException {
    URL url = new URL(urlString);
    URLConnection conn = url.openConnection();
    return conn.getInputStream();
  }

  /**
   * 调用{@link Resources#getUrlAsStream}获取一个远程URL链接的字节输入流，然后将其转换成字符输入流，<br>
   * 如果{@link Resources#charset}不是NULL，就使用该字符集进行字符流的转换
   *
   * @param urlString 要加载的字节输入流的远程URL链接
   * @return A Reader with the data from the URL
   * @throws IOException If the resource cannot be found or read
   */
  public static Reader getUrlAsReader(String urlString) throws IOException {
    Reader reader;
    if (charset == null) {
      reader = new InputStreamReader(getUrlAsStream(urlString));
    } else {
      reader = new InputStreamReader(getUrlAsStream(urlString), charset);
    }
    return reader;
  }

  /**
   * 调用{@link Resources#getUrlAsStream}获取一个远程URL链接的字节输入流，然后将其转换成字符输入流，<br>
   * 然后调用{@link Properties#load(InputStream)}将字节流加载成一个Properties对象
   *
   * @param urlString 要加载字节流的远程URL链接
   * @return A Properties object with the data from the URL
   * @throws IOException If the resource cannot be found or read
   */
  public static Properties getUrlAsProperties(String urlString) throws IOException {
    Properties props = new Properties();
    try (InputStream in = getUrlAsStream(urlString)) {
      props.load(in);
    }
    return props;
  }

  /**
   * 直接调用{@link ClassLoaderWrapper#classForName(String)}来加载类
   *
   * @param className 要加载类的全限定名
   * @return 加载的类
   * @throws ClassNotFoundException If the class cannot be found (duh!)
   */
  public static Class<?> classForName(String className) throws ClassNotFoundException {
    return classLoaderWrapper.classForName(className);
  }

  public static Charset getCharset() {
    return charset;
  }

  public static void setCharset(Charset charset) {
    Resources.charset = charset;
  }

}
