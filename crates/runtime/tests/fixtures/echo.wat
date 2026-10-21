(component
  (core module $m
    (memory (export "memory") 1)
    (global $next (mut i32) (i32.const 1024))
    (func (export "realloc") (param i32 i32 i32 i32) (result i32)
      (local $p i32)
      global.get $next local.tee $p local.get 3 i32.add global.set $next local.get $p)
    (func (export "handle") (param $p i32) (param $n i32) (result i32)
      i32.const 0 i32.const 0 i32.store
      i32.const 4 local.get $p i32.store
      i32.const 8 local.get $n i32.store
      i32.const 0))
  (core instance $i (instantiate $m))
  (type $h (func (param "input" string) (result (result string (error string)))))
  (func $handle (type $h) (canon lift (core func $i "handle")
    (memory (core memory $i "memory")) (realloc (core func $i "realloc"))))
  (instance $handler (export "handle" (func $handle)))
  (export "merezha:service/handler@0.1.0" (instance $handler)))
