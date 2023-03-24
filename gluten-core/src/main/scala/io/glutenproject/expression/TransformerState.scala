package io.glutenproject.expression

object TransformerState {
  private lazy val validationState = new ThreadLocal[Boolean] {
    override def initialValue: Boolean = false
  }
  def underValidationState: Boolean = validationState.get()

  def enterValidation = validationState.set(true)
  def finishValidation = validationState.set(false)
}
