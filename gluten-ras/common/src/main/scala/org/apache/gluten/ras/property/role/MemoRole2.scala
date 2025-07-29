package org.apache.gluten.ras.property.role

sealed trait MemoRole2

object MemoRole2 {
  case object Hub extends MemoRole2
  case object User extends MemoRole2
  case object Leaf extends MemoRole2
}
