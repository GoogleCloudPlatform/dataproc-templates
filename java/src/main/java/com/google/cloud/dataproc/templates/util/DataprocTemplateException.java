/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataproc.templates.util;

public class DataprocTemplateException extends RuntimeException {

  /**
   * Constructs a new DataprocTemplateException with null as its detail message. The cause is not
   * initialized, and may subsequently be initialized by a call to initCause.
   */
  public DataprocTemplateException() {
    super();
  }

  /**
   * Constructs a new DataprocTemplateException with the specified detail message. The cause is not
   * initialized, and may subsequently be initialized by a call to initCause.
   *
   * @param message – the detail message. The detail message is saved for later retrieval by the
   *     getMessage() method.
   */
  public DataprocTemplateException(String message) {
    super(message);
  }

  /**
   * Constructs a new DataprocTemplateException with the specified detail message and cause. Note
   * that the detail message associated with cause is not automatically incorporated in this runtime
   * exception's detail message. Params:
   *
   * @param message – the detail message (which is saved for later retrieval by the getMessage()
   *     method).
   * @param cause – the cause (which is saved for later retrieval by the getCause() method). (A null
   *     value is permitted, and indicates that the cause is nonexistent or unknown.)
   */
  public DataprocTemplateException(String message, Throwable cause) {
    super(message, cause);
  }
}
