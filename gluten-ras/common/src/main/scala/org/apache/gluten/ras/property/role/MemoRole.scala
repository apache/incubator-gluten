package org.apache.gluten.ras.property.role

sealed trait MemoRole

private[ras] object MemoRole {
  case object Hub extends MemoRole
  case object User extends MemoRole
  case object Leaf extends MemoRole
}
