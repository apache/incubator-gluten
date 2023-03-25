package io.glutenproject.expression

object TransformerState {
  private lazy val validationState = new ThreadLocal[Integer] {
    override def initialValue: Integer = 0
  }
  def underValidationState: Boolean = validationState.get() > 0

  def enterValidation = validationState.set(validationState.get() + 1)
  def finishValidation = validationState.set(validationState.get() - 1)
}
